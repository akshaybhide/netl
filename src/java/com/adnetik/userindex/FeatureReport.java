
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
import com.adnetik.shared.DbUtil.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.analytics.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

public class FeatureReport
{	
	public enum FeatTabCol { report_idInt, feat_nameStr, feat_codeStr, mut_infoDbl, pos_countInt, neg_countInt }
	
	String _blockEnd;
	
	SimpleMail _logMail;
	
	List<PosRequest> _posList = Util.vector();
	
	SortedMap<NegRequest, SortedSet<PosRequest>> _neg2posMap = Util.treemap();
	
	FileSystem _fSystem;
	
	int _maxUser; 
	
	public static void main(String[] args) throws Exception
	{
		ArgMap argmap = Util.getClArgMap(args);
		
		// Crontab will run using dow=wed maybe
		if(argmap.containsKey("dow"))
		{
			ShortDowCode targdow = ShortDowCode.valueOf(argmap.get("dow"));
			ShortDowCode currdow = TimeUtil.getDowCode(TimeUtil.getTodayCode());
			
			if(targdow != currdow)
			{ 
				Util.pf("DOW target is %s, today is %s\n", targdow, currdow);	
				return;
			}
		}
		
		String blockend = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());				
		blockend =  argmap.getDayCode("blockend", blockend); 
		Integer maxuser = argmap.getInt("maxuser", UserIndexUtil.MAX_USERS_FEATURE);
		
		UFeatManager.initSing(blockend);
		
		FeatureReport frep = new FeatureReport(blockend, maxuser);		
		
		// set up list of targets to use
		frep.initMapData(argmap.getString("targlist", "all"));
		
		frep.runProcess();
	}	
	
	FeatureReport(String blockend, int maxuser) throws IOException
	{
		UserIndexUtil.assertValidBlockEnd(blockend);
		
		_blockEnd = blockend;
		_logMail = new SimpleMail(UserIndexUtil.AIDX_WEEKLY_CODE + " BasicFeatureReport for " + _blockEnd);
		
		_fSystem = FileSystem.get(new Configuration());
		
		_maxUser = maxuser;
	}

	
	void initMapData(String targlist) throws IOException
	{
		Set<PosRequest> posreqset = ListInfoManager.getSing().getPosRequestSet();
		if(!("all".equals(targlist)))
		{
			posreqset.clear();
			for(String onelist : targlist.split(","))
			{ 
				posreqset.add(ListInfoManager.getSing().getPosRequest(onelist));	
			}
			
			Util.pf("Using special target list %s\n", posreqset);		
		}
		
		// eg. /userindex/precomp/2013-03-17/specpcc_NL.prec
		
		String precpatt = Util.sprintf("/userindex/precomp/%s/*.prec", _blockEnd);
		LinkedList<Path> pathlist = Util.linkedlist();
		pathlist.addAll(HadoopUtil.getGlobPathList(_fSystem, precpatt));
		int prevsize = pathlist.size();
		
		Util.massert(prevsize > 10, "Found too few paths %d", prevsize);
		
		for(int i = 0; i < prevsize*2 && !pathlist.isEmpty(); i++)
		{
			Path onepath = pathlist.pollFirst();
			
			String listcode = onepath.getName().split("\\.")[0];
			
			/*
			if(!ListInfoManager.getSing().haveRequest(listcode))
			{ 
				_logMail.pf("WARNING: no scan request found for listcode %s\n", listcode);
				continue;
			}
			*/
			
			Util.massert(ListInfoManager.getSing().haveRequest(listcode),
				"Scan request %s not found in ListInfoManager", listcode);
						
			ScanRequest scanreq = ListInfoManager.getSing().getRequest(listcode);
			
			if(scanreq instanceof NegRequest)
			{ 
				_neg2posMap.put((NegRequest) scanreq, new TreeSet<PosRequest>()); 
				// _logMail.pf("Found negative request %s\n", scanreq.getListCode());
				continue;
			}
			
			PosRequest preq = (PosRequest) scanreq;
			
			if(!posreqset.contains(preq))
				{ continue; }
					
			if(_neg2posMap.containsKey(preq.getNegRequest()))
				{ _neg2posMap.get(preq.getNegRequest()).add(preq); }
			else 
				{ pathlist.addLast(onepath); }
		}

		Util.massert(pathlist.isEmpty(), "Still have %d paths in pathlist", pathlist.size());
		
		for(NegRequest negreq : _neg2posMap.keySet())
		{
			Util.pf("Found %d requests for negative %s\n", _neg2posMap.get(negreq).size(), negreq.getListCode());	
		}
	}
	
	void runProcess() throws IOException
	{
				
		for(NegRequest negreq : _neg2posMap.keySet())
		{
			_logMail.pf("Generating %d reports for negreq %s, blockend %s\n", 
				_neg2posMap.get(negreq).size(), negreq.getListCode(), _blockEnd);
			
			// This will happen if we do a targeted FRep generation
			if(_neg2posMap.get(negreq).isEmpty())
				{ continue;} 
			
			PrecompFeatPack negpack = new PrecompFeatPack(_blockEnd, negreq.getListCode());
			_logMail.pf("Loading negative list %s...", negreq.getListCode());
			negpack.loadFromHdfs(_maxUser);
			_logMail.pf(" ... done\n");
			
			// Okay, we are not going to do the peel/remove thing anymore. 
			// Now will only have two big PFPs in memory at any given time
			for(PosRequest posreq : _neg2posMap.get(negreq))
			{
				PrecompFeatPack pospack = new PrecompFeatPack(_blockEnd, posreq.getListCode());
				
				_logMail.pf("Loading positive list %s...", posreq.getListCode());
				pospack.loadFromHdfs(_maxUser);
				_logMail.pf(" ... done\n");
				
				{
					_logMail.pf("Generating report comparison %s xx %s...", 
						posreq.getListCode(), negreq.getListCode());
					
					FeatRepItem frepitem = new FeatRepItem(pospack, negpack);
					
					String outputfile = UserIndexUtil.getUserFeaturePath(posreq.getListCode(), 
						negreq.getListCode(), _blockEnd);
					
					frepitem.writeToCsv(outputfile);
					frepitem.writeToDb(_blockEnd);
					_logMail.pf(" ... done\n");
				}
			}	
		}
		
		_logMail.send2admin();		
	}
	
	static void writeToCsv(List<Pair<UserFeature, AdaBoost.HitPack>> feathitlist, String outputFile) throws IOException
	{
		writeToCsv(feathitlist, outputFile, false);
	}
	
	static void writeToCsv(List<Pair<UserFeature, AdaBoost.HitPack>> feathitlist, String outputFile, boolean extraCol) throws IOException
	{
		// Create directory if necessary
		FileUtils.createDirForPath(outputFile);
		
		PrintWriter pwrite = new PrintWriter(outputFile);
		List<String> clist = Util.vector();
		{
			
			clist.addAll(Arrays.asList(new String[] { "Feature Name", "Feature Code", "Direction", 
			"Discriminatory Power", "Feature Power", "%% Seed ", "%% Control " }));
			
			if(extraCol)
			{ 
				clist.addAll(Arrays.asList(new String[] { " Num Seed ", " Num Control ", "Seed Total", "Control Total"}));
			}
		}
		pwrite.printf("%s\n", Util.join(clist, ","));
		// pwrite.printf("Feature Name,Feature Code,Direction, Discriminatory Power, Feature Power, %%Positive, %%Negative\n");
		
		// Util.pf("Hit pack is %s\n", feathitlist.get(0)._2);
		
		int seedtotal = feathitlist.get(0)._2.getSeedTotal();
		int ctrltotal = feathitlist.get(0)._2.getControlTotal();
		
		for(Pair<UserFeature, AdaBoost.HitPack> sorthit : feathitlist)
		{
			BinaryFeature<UserPack> relfeat = sorthit._1;
			AdaBoost.HitPack ahp = sorthit._2;	
			
			List<String> rowlist = Util.vector();
			rowlist.add(sorthit._1.toString().replace(',', '_'));
			rowlist.add(relfeat.getCode().toString());
			
			//FEature Name, Category, Direction	Discriminatory Power	Feature Power	Positive %	Random %
			// pwrite.printf("%s,%s,", sorthit._1.replace(',', '_'), relfeat.getCode());
			
			{
				int dir = ahp.getPercPos() >= ahp.getPercNeg() ? 1 : -1;
				double discpower = ahp.getMiScore();
				double featpower = discpower * dir;
				
				rowlist.add(""+dir);
				rowlist.add(Util.sprintf("%.04f", discpower));
				rowlist.add(Util.sprintf("%.04f", featpower));
				rowlist.add(Util.sprintf("%.04f", ahp.getPercPos()));
				rowlist.add(Util.sprintf("%.04f", ahp.getPercNeg()));	
			}
			
			if(extraCol)
			{
				rowlist.add(""+ahp.getSeedPositive());
				rowlist.add(""+ahp.getControlPositive());
				rowlist.add(""+ahp.getSeedTotal());
				rowlist.add(""+ahp.getControlTotal());
				
				//Util.massert(seedtotal == ahp.getSeedTotal(), "Mismatch in seed totals for feature %s, found %d should be %d", 
				//	sorthit._1, seedtotal, ahp.getSeedTotal());
				// Util.massert(ctrltotal == ahp.getControlTotal(), "Mismatch in control totals");
			}
			
			pwrite.printf("%s\n", Util.join(rowlist, ","));
			// pwrite.printf("%.04f,%.04f\n", ahp.getPercPos(), ahp.getPercNeg());
		}
		
		pwrite.close();
		
		// Util.pf("\nFinished generating output.\n\n");			
	}
	
	
	
	
	private static class FeatRepItem
	{
		private Map<String, boolean[]> _boolMap = Util.treemap();
		
		private boolean[] _targVec;
		
		private String _dayCode;
		
		private PosRequest _posReq;
		// private String _negCode;
		// private String _posCode;
		
		private int _negCount;
		private int _posCount;
		
		public FeatRepItem(PrecompFeatPack poscalc, PrecompFeatPack negcalc)
		{
			Util.massert(poscalc.getBlockEnd().equals(negcalc.getBlockEnd()));
			_dayCode = poscalc.getBlockEnd();
			
			_posReq = (PosRequest) poscalc._scanReq;
			
			// _posCode = poscalc.lstCode;
			// _negCode = negcalc.lstCode;
			
			_posCount = poscalc.numUsers();
			_negCount = negcalc.numUsers();
			
			Util.pf("\nCombining feature results...");
			_targVec = PrecompFeatPack.genTargVec(poscalc, negcalc);
			_boolMap = PrecompFeatPack.combine(poscalc, negcalc);
			Util.pf(" ... done\n");		
			
			Util.pf("Built Feature Report object, has %d users and %d features\n",
				_targVec.length, _boolMap.size());
		}	
		
		private SortedSet<Pair<Double, UserFeature>> getSortFeatSet()
		{
			System.out.printf("\nReading %d data objects and %d features",
				_targVec.length, _boolMap.size());		
			
			// Order high to low in terms of score
			SortedSet<Pair<Double, UserFeature>> sortfeat = new TreeSet<Pair<Double, UserFeature>>(Collections.reverseOrder());
			
			Collection<UserFeature> ufeatset = UFeatManager.getSing().getFeatSet(_posReq);
			
			// Never DQ features at this phase - put geo and stuff in the report, maybe it 
			// won't be relevant. 
			// Set<FeatureCode> featcodeset = ListInfoManager.getSing().getLearnCodes4List(_posReq);
					
			//for(String funcname : StrayerFeat.getFeatMap().keySet())
			for(UserFeature ufeat : ufeatset)
			{
				// Util.pf("Evaluating for function %s, code %s\n", funcname, StrayerFeat.getFeatMap().get(funcname).getCode());
				AdaBoost.HitPack ahp = new AdaBoost.HitPack(Util.safeget(_boolMap, ufeat.getNameKey()), _targVec);
				Pair<Double, UserFeature> p = Pair.build(ahp.getMiScore(), ufeat);
				sortfeat.add(p);
			}			
			
			// Make sure we didn't lose anything in the set adds
			Util.massert(sortfeat.size() == ufeatset.size());	
			
			return sortfeat;
		}
		
		void writeToCsv(String outputFile) throws IOException
		{	
			List<Pair<UserFeature, AdaBoost.HitPack>> feathitlist = Util.vector();
			SortedSet<Pair<Double, UserFeature>> sortfeat = getSortFeatSet();
			
			for(Pair<Double, UserFeature> p : sortfeat)
			{
				UserFeature relfeat = p._2;
				AdaBoost.HitPack ahp = new AdaBoost.HitPack(Util.safeget(_boolMap, p._2.getNameKey()), _targVec);	
				feathitlist.add(Pair.build(relfeat, ahp));
			}
			
			FeatureReport.writeToCsv(feathitlist, outputFile);
		}			
		
		private void updateUserCounts(int reportid)
		{
			String inupsql = "INSERT INTO user_counts (report_id, pos_user_total, neg_user_total) ";
			inupsql += Util.sprintf(" VALUES ( %d, %d, %d )  ", reportid, _posCount, _negCount);
			inupsql += Util.sprintf(" ON DUPLICATE KEY UPDATE pos_user_total = %d, neg_user_total = %d  ", _posCount, _negCount);
			
			// Util.pf("Ins UP sql is:\n%s\n", inupsql);
			
			DbUtil.execSqlUpdate(inupsql, new UserIdxDb());		
			
		}	
		
		void writeToDb(String blockend) throws IOException
		{
			int reportid = UserIdxDb.lookupCreateRepId(_dayCode, _posReq);
			
			updateUserCounts(reportid);			
			
			// Delete old data
			{
				String delsql = Util.sprintf("DELETE FROM feature_table WHERE report_id = %d", reportid);
				int delrows = DbUtil.execSqlUpdate(delsql, new UserIdxDb());
				Util.pf("Deleted %d previous rows for reportid=%d", delrows, reportid);
			}
			
			String tempfilepath = Util.sprintf("%s/db_temp/frepinf_%s_tmp.tsv", UserIndexUtil.LOCAL_UINDEX_DIR, blockend);
			
			SortedSet<Pair<Double, UserFeature>> sortfeat = getSortFeatSet();

			// Set the size of the infspooler to the number of features, just do one big batch upload
			InfSpooler infspooler = new InfSpooler(FeatTabCol.values(), tempfilepath, new UserIdxDb(), "feature_table");
			infspooler.setBatchSize(sortfeat.size()+1);
			
			for(Pair<Double, UserFeature> p : sortfeat)
			{
				UserFeature ufeat = p._2;
				AdaBoost.HitPack ahp = new AdaBoost.HitPack(Util.safeget(_boolMap, p._2.getNameKey()), _targVec);	
				
				infspooler.setInt(FeatTabCol.report_idInt, reportid);
				infspooler.setStr(FeatTabCol.feat_nameStr, ufeat.toString());
				infspooler.setStr(FeatTabCol.feat_codeStr, ufeat.getCode().toString());
			
				infspooler.setDbl(FeatTabCol.mut_infoDbl, ahp.getMiScore());
				infspooler.setInt(FeatTabCol.pos_countInt, ahp.getCount(true, true ));
				infspooler.setInt(FeatTabCol.neg_countInt, ahp.getCount(true, false));
				
				infspooler.flushRow();
			}			
			
			infspooler.finish(true);
		}
	}
	

}
