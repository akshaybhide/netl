
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.data_management.*;
import com.adnetik.data_management.ExelateDataMan.*;
import com.adnetik.data_management.BluekaiDataMan.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.AdaBoost.HitPack;
import com.adnetik.userindex.ScanRequest.*;

public class ThirdPartyFRep
{	
	private String _canStartDay;

	private enum LocalFile { stagecopy, infgimp };
	
	// Stats about how many users we found with given combination of +BK,+EX, +EX,-BK, +BK,-EX, -EX,-BK
	private int _bothPackUsers = 0;
	private int _exOnlyUsers = 0;
	private int _blOnlyUsers = 0;
	private int _noDataUsers = 0;
	
	SortedMap<String, BluekaiFeatureFunc> _bkFeatMap = Util.treemap();
	SortedMap<String, ExelateFeatureFunc> _exFeatMap = Util.treemap();
	
	SimpleMail _logMail;
	
	SubDataStore _subData;
	
	private int _maxUser; 
	
	private enum DbFieldEnum { REPORT_IDInt, FEAT_NAMEStr, FEAT_CODEStr, MUT_INFODbl, 
		TRUE4SEEDInt, TRUE4CTRLInt, TOTALSEEDInt, TOTALCTRLInt,
		P3_SEG_IDInt, P3_DATA_CODEStr
		};
	
	public static final String PARTY3_TABLE_NAME = "party3_report";
	
	public static void main(String[] args) throws IOException
	{
		// Crontab will run using dow=wed maybe
		ArgMap optargs = Util.getClArgMap(args);
		
		if(optargs.containsKey("dow"))
		{
			ShortDowCode targdow = ShortDowCode.valueOf(optargs.get("dow"));
			ShortDowCode currdow = TimeUtil.getDowCode(TimeUtil.getTodayCode());
			
			if(targdow != currdow)
			{ 
				Util.pf("DOW target is %s, today is %s\n", targdow, currdow);	
				return;
			}
		}		
		
		String canstart;
		{
			// TODO: replace this with a check that determines if the start day has already passed
			String daycode = "yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0];
			Util.massert(TimeUtil.checkDayCode(daycode), "Invalid day code %s", daycode);
			canstart = UserIndexUtil.getBlockStartForDay(daycode);
		}
		
		Util.pf("Running scan for block start/end [%s, %s]\n", 
			canstart, UserIndexUtil.getNextBlockEnd(canstart));

		Integer maxuser = optargs.getInt("maxuser", Integer.MAX_VALUE);
		boolean save2disk = optargs.getBoolean("save2disk", false);
		boolean loadfromdisk = optargs.getBoolean("loadfromdisk", false);
		
		ThirdPartyFRep tpfr = new ThirdPartyFRep(canstart, maxuser);
		
		if(loadfromdisk)
		{
			// If this is true, we just load the SubDataStore from the disk
			tpfr.loadSubFromDisk();
			
		} else {

			tpfr.grabNSort();
						
			tpfr.initQueues();
						
			tpfr.doScan();
		}
		
		if(save2disk)
			{ tpfr._subData.writeToDisk(); }
		
		tpfr.generateReports();
		tpfr.cleanUp();
		tpfr._logMail.send2admin();
	}	

	
	public ThirdPartyFRep(String csd, Integer mxuser)
	{
		_maxUser = (mxuser == null ? Integer.MAX_VALUE : mxuser);
		
		Util.massert(TimeUtil.checkDayCode(csd), "Bad day code %s", csd);
		Util.massert(UserIndexUtil.isBlockStartDay(csd), "Day code %s is not a block-start day", csd);
		
		_canStartDay = csd;
		
		_logMail = new SimpleMail(Util.sprintf("%s ThirdPartyFRep for Start Day %s", UserIndexUtil.AIDX_WEEKLY_CODE, csd));
		
		for(UserFeature exfeat : UFeatManager.FeatureCompiler.getExelateFeatSet())
		{
			Util.putNoDup(_exFeatMap, exfeat.getNameKey(), (ExelateFeatureFunc) exfeat);
		}
		
		for(UserFeature bkfeat : UFeatManager.FeatureCompiler.getBluekaiFeatSet())
		{			
			Util.putNoDup(_bkFeatMap, bkfeat.getNameKey(), (BluekaiFeatureFunc) bkfeat);
		}		
		
		Util.pf("Initialized feature maps, %d bluekai and %d exelate features\n", 
			_bkFeatMap.size(), _exFeatMap.size());
		
		_subData = new SubDataStore();
	}
	
	private void cleanUp()
	{
		for(LocalFile lfile : LocalFile.values())
		{
			File f = new File(getLocalFilePath(lfile));
			if(f.exists())
			{
				_logMail.pf("Deleting local file %s\n", f.getAbsolutePath());
				f.delete(); 
			}
		}
		
	}
	
	private void loadSubFromDisk()
	{
		_subData = new SubDataStore();
		_subData.loadFromDisk();
		_logMail.pf("Loaded SubDataStore from disk, found %d list codes", _subData.getListenCodeList().size());
	}
	
	private void deleteOldData(int reportid)
	{
		String delsql = Util.sprintf("DELETE FROM %s WHERE report_id = %d", PARTY3_TABLE_NAME, reportid);
		int delrows = DbUtil.execSqlUpdate(delsql, new UserIdxDb());
		
		_logMail.pf("Deleted %d old rows of data for report_id=%d\n", delrows, reportid); 
	}
	
	private void generateReports() throws IOException
	{
		DbUtil.InfSpooler dbis = new DbUtil.InfSpooler(DbFieldEnum.values(), 
			getLocalFilePath(LocalFile.infgimp), new UserIdxDb(), PARTY3_TABLE_NAME);
		dbis.setLogMail(_logMail);
				
		for(ScanRequest poslist : _subData.getListenCodeList())
		{
			if(!(poslist instanceof PosRequest))
				{ continue; }
			
			PosRequest posreq = (PosRequest) poslist;
			
			// Util.pf("List set is %s\n", _subData.getListenCodeList());
			NegRequest negreq = posreq.getNegRequest();
			
			Map<String, HitPack> feathitmap = Util.treemap();
			for(String bluefeat : _bkFeatMap.keySet())
			{
				HitPack hpack = new HitPack();
				_subData.add2HitPack(posreq, bluefeat, hpack, true);
				_subData.add2HitPack(negreq, bluefeat, hpack, false);	
				feathitmap.put(bluefeat, hpack);
			}
			
			for(String exelfeat : _exFeatMap.keySet())
			{
				HitPack hpack = new HitPack();
				_subData.add2HitPack(posreq, exelfeat, hpack, true);
				_subData.add2HitPack(negreq, exelfeat, hpack, false);	
				feathitmap.put(exelfeat, hpack);
			}			
			
			
			SortedSet<Pair<Double, UserFeature>> sortedhit = new TreeSet<Pair<Double, UserFeature>>(Collections.reverseOrder());
			for(String featname : feathitmap.keySet())
			{
				UserFeature ufeat = UserFeature.buildFromNameKey(featname);
				HitPack hpack = feathitmap.get(featname);
				double hscore = hpack.getMiScore();
				sortedhit.add(Pair.build(hscore, ufeat));
			}
			
			List<Pair<UserFeature, HitPack>> feathitlist = Util.vector();	
			for(Pair<Double, UserFeature> onepair : sortedhit)
			{
				HitPack hpack = feathitmap.get(onepair._2.getNameKey());
				feathitlist.add(Pair.build(onepair._2, hpack));
			}
			
			Util.pf("Feat list size is %d\n", feathitlist.size());
			
			String party3path = UserIndexUtil.get3PartyFeaturePath(posreq, UserIndexUtil.getNextBlockEnd(_canStartDay));
			FeatureReport.writeToCsv(feathitlist, party3path, true);
			upload2db(dbis, feathitlist, poslist.getListCode());
			_logMail.pf("Generated 3party FRep for poslist=%s, neglist=%s\n", posreq.getListCode(), negreq.getListCode());
		}
		
		dbis.finish();
	}
	
	private void upload2db(DbUtil.InfSpooler dbis, List<Pair<UserFeature, HitPack>> feathitlist, String poscode) throws IOException
	{
		PosRequest posreq = ListInfoManager.getSing().getPosRequest(poscode);
		
		int reportid = UserIdxDb.lookupCreateRepId(UserIndexUtil.getNextBlockEnd(_canStartDay), posreq);

		deleteOldData(reportid);
		
		for(Pair<UserFeature, HitPack> onepair : feathitlist)
		{
			HitPack hp = onepair._2;
			// private enum DbFieldEnum { FEAT_NAMESTR, FEAT_CODESTR, MUT_INFODBL, TRUE4SEEDINT, TRUE4CTRLINT, TOTALSEED, TOTALCTRL };
			
			UserFeature onefeat = onepair._1;
			Util.massert(onefeat != null, "Could not find feature with name %s", onepair._1);
			
			dbis.setInt(DbFieldEnum.REPORT_IDInt, reportid);			
			dbis.setStr(DbFieldEnum.FEAT_NAMEStr, onepair._1.toString());
			dbis.setStr(DbFieldEnum.FEAT_CODEStr, onefeat.getCode().toString());
			
			
			dbis.setDbl(DbFieldEnum.MUT_INFODbl, hp.getMiScore());
			dbis.setInt(DbFieldEnum.TRUE4SEEDInt, hp.getSeedPositive());
			dbis.setInt(DbFieldEnum.TRUE4CTRLInt, hp.getControlPositive());
			dbis.setInt(DbFieldEnum.TOTALSEEDInt, hp.getSeedTotal());
			dbis.setInt(DbFieldEnum.TOTALCTRLInt, hp.getControlTotal());
			
			{
				UserFeature.HasParty3Info hp3info = Util.cast(onefeat);
				Pair<Part3Code,Integer> seginfo = hp3info.getSegmentInfo();
				dbis.setStr(DbFieldEnum.P3_DATA_CODEStr, seginfo._1.toString());
				dbis.setInt(DbFieldEnum.P3_SEG_IDInt, seginfo._2);
			}
			
			dbis.flushRow();
		}
	}
	
	private void initQueues() throws IOException
	{
		BluekaiDataMan.setSingQ(TimeUtil.dayBefore(_canStartDay));
		ExelateDataMan.setSingQ(TimeUtil.dayBefore(_canStartDay));
		
		// BluekaiDataMan.setSingQ(_canStartDay);
		// ExelateDataMan.setSingQ(_canStartDay);
		
	}

	private String getLocalFilePath(LocalFile lf)
	{
		return Util.sprintf("%s/3partyFrep/%s_%s.txt", 
			UserIndexUtil.LOCAL_UINDEX_DIR, lf, _canStartDay);
	}
	
	private void grabNSort() throws IOException
	{
		FileUtils.deleteIfExists(getLocalFilePath(LocalFile.stagecopy));

		FileSystem fsys = FileSystem.get(new Configuration());
		
		_logMail.pf("Grabbing staging info pack...");
		LookupPack lpack = new LookupPack(_canStartDay);
		lpack.readFromHdfs(fsys);
		lpack.write2File(new File(getLocalFilePath(LocalFile.stagecopy)));

		_logMail.pf(" done");		
	}
	
	private void doScan() throws IOException
	{
		String curid = null;
		Set<ScanRequest> curset = Util.treeset();
		
		BufferedReader bread = FileUtils.getReader(getLocalFilePath(LocalFile.stagecopy));
		
		int numusers = 0;
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			String[] wtp_list = oneline.trim().split("\t");	
			
			if(wtp_list.length != 2)
			{
				Util.pf("Error in format of local stage copy, skipping row");
				continue;
			}

			String newid = wtp_list[0];
			
			Util.massert(ListInfoManager.getSing().haveRequest(wtp_list[1]),
				"No request found with listcode %s", wtp_list[1]);
			
			ScanRequest scanreq = ListInfoManager.getSing().getRequest(wtp_list[1]);
			
			if(!newid.equals(curid))
			{
				if(curid != null)
				{
					Util.massert(!curset.isEmpty(), "Empty listcode set");
					
					ExUserPack expack = ExelateDataMan.getSingQ().lookup(curid);
					BluserPack blpack = BluekaiDataMan.getSingQ().lookup(curid);
					
					if(expack != null && blpack != null)
						{ _bothPackUsers++; }
					else if(expack != null) // blpack == null
						{ _exOnlyUsers++; }
					else if(blpack != null) // expack == null
						{ _blOnlyUsers++; }
					else
						{ _noDataUsers++; }				
					
					_subData.inc4User(curset, expack, blpack);
				}
				
				// Util.pf("Found newid %s\n", newid);
				
				// Refresh
				curid = newid;
				curset.clear();
				
				numusers++;
				if((numusers % 1000) == 0)
				{
					Util.pf("Finished with %d users\n", numusers);	
					// _subData.reportData();
				}				
			}
			
			if(numusers > _maxUser)
				{ break; }
			
			Util.massert(!curset.contains(scanreq), "Found multiple instances of listcode %s", scanreq.getListCode());	
			curset.add(scanreq);
		}
		
		_logMail.pf("Found %d BOTH, %d EX ONLY, %d BL ONLY, %d NO DATA, %d total\n",
			_bothPackUsers, _exOnlyUsers, _blOnlyUsers, _noDataUsers, numusers);
		
		ExelateDataMan.getSingQ().close();
		BluekaiDataMan.getSingQ().close();
		bread.close();
	}
	
	private class SubDataStore
	{
		// ListCode --> BinaryFeature
		TreeMap<ScanRequest, TreeMap<String, Integer>> _trueHitMap = Util.treemap();	
		TreeMap<ScanRequest, TreeMap<String, Integer>> _falsHitMap = Util.treemap();	
		
		private void initListPack(ScanRequest preq)
		{
			//Util.massert(!_dataPack.containsKey(listcode), "Listcode %s already present", listcode);
			//_dataPack.put(listcode, new TreeMap<String, int[]>());
			
			_trueHitMap.put(preq, new TreeMap<String, Integer>());
			_falsHitMap.put(preq, new TreeMap<String, Integer>());
			
			for(String bluefeat : _bkFeatMap.keySet())
			{
				//Util.pf("Going to inc for Listcode=%s, bkfeat=%s\n", listcode, bluefeat);
				//_dataPack.get(listcode).put(bluefeat, new int[2]);
				_trueHitMap.get(preq).put(bluefeat, 0);
				_falsHitMap.get(preq).put(bluefeat, 0);
			}
			
			for(String exelfeat : _exFeatMap.keySet())
			{
				// _dataPack.get(listcode).put(exelfeat, new int[2]);
				_trueHitMap.get(preq).put(exelfeat, 0);
				_falsHitMap.get(preq).put(exelfeat, 0);				
			}			
		}
		
		private void inc4User(Collection<ScanRequest> listenset, ExUserPack expack, BluserPack bkpack)
		{
			// Util.pf("Inc4User, listenset=%s\n", listenset);
			
			// Initialize the listencodes if they are not already present
			for(ScanRequest scanreq : listenset)
			{
				if(!_trueHitMap.containsKey(scanreq))
					{ initListPack(scanreq); }
			}
			
			if(expack != null)
			{
				//cUtil.pf("FOund non-null ExPack for user\n");
				
				for(String exfeat : _exFeatMap.keySet())
				{
					boolean featres = _exFeatMap.get(exfeat).exEval(expack);	
					
					for(ScanRequest scanreq : listenset)
					{
						Util.incHitMap((featres ? _trueHitMap : _falsHitMap).get(scanreq), exfeat, 1);						
					}
				}
			}
			
			if(bkpack != null)
			{
				for(String bkfeat : _bkFeatMap.keySet())
				{
					// Util.pf("Feature name is %s, feature is %s\n", bkfeat, _bkFeatMap.get(bkfeat));
					// BluekaiFeatureFunc bff = _bkFeatMap.get(bkfeat);
					
					
					boolean featres = _bkFeatMap.get(bkfeat).bkEval(bkpack);	
					
					for(ScanRequest scanreq : listenset)
					{
						//Util.massert(_dataPack.containsKey(listcode) 
						//	&& _dataPack.get(listcode).containsKey(bkpack));
						//int[] respack = _dataPack.get(listcode).get(bkpack);
						
						Util.incHitMap((featres ? _trueHitMap : _falsHitMap).get(scanreq), bkfeat, 1);
						//Util.pf("Featres is %b, curpack is %d,%d\n", 
						//	featres, respack[0], respack[1]);
						
						
						// respack[(featres ? 0 : 1)] += 1;
					}
				}
			}			
			
		}
		
		private void add2HitPack(ScanRequest listcode, String fname, HitPack hpack, boolean istarget)
		{
			if(!istarget)
				{ Util.massert(listcode.getStageType() == StagingType.negative, "Non target must be staging type negative"); }
			
			for(int i = 0; i < _trueHitMap.get(listcode).get(fname); i++)
				{ hpack.inc4predTarg(true, istarget); }
			
			for(int i = 0; i < _falsHitMap.get(listcode).get(fname); i++)
				{ hpack.inc4predTarg(false, istarget); }
			
		}
		
		private String getHitMapPath(boolean istrue)
		{
			return Util.sprintf("%s/3partyFrep/%shitmap.ser", 
				UserIndexUtil.LOCAL_UINDEX_DIR, (istrue ? "true" : "fals"));
			
		}
		
		private void writeToDisk() throws IOException
		{
			FileUtils.serialize(_trueHitMap, getHitMapPath(true ));
			FileUtils.serialize(_falsHitMap, getHitMapPath(false));
			
			Util.pf("Wrote hit maps to disk\n");
		}
		
		private void loadFromDisk()
		{
			_trueHitMap = FileUtils.unserializeEat(getHitMapPath(true ));
			_falsHitMap = FileUtils.unserializeEat(getHitMapPath(false));
			
			Util.pf("Loaded %d posmap, %d negmap size results\n", _trueHitMap.size(), _falsHitMap.size());
		}
		
		
		List<ScanRequest> getListenCodeList()
		{
			return new Vector<ScanRequest>(_trueHitMap.keySet());
		}
		
		private void reportData()
		{
			for(ScanRequest scanreq : _trueHitMap.keySet())
			{
				for(String featname : _trueHitMap.get(scanreq).keySet())
				{
					int poscount = _trueHitMap.get(scanreq).get(featname);
					int negcount = _falsHitMap.get(scanreq).get(featname);
					
					// int[] respack = _dataPack.get(listcode).get(featname);
					Util.pf("For listcode %s, featname %s had %d/%d results\n",
						scanreq.getListCode(), featname, poscount, negcount);
				}
			}
		}
	}
}
