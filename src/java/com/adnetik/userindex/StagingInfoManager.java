
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.DbUtil.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;

// I don't really want to include these, don't make this package depend on BMETL
import com.adnetik.bm_etl.DatabaseBridge;
import com.adnetik.bm_etl.BmUtil.*;


public class StagingInfoManager
{		
	
	private String _dayCode;
	
	private SimpleMail _logMail;
	
	private FileSystem _fSystem;
	
	private StatusReportMan _repManager = new StatusReportMan();
	
	private Map<String, Integer> _listCountMap = Util.treemap();
	
	private MailPackage _mailPack;
	
	public static void main(String[] args) throws IOException
	{
		if(args.length < 1)
		{ 
			Util.pf("Usage: StagingInfoManager <daycode>");
			return;
		}
		
		ArgMap optmap = Util.getClArgMap(args);
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid Day Code %s", daycode);
		
		// These are both true by default. Useful for testing purposes to be able to turn them off.
		boolean dostrip = optmap.getBoolean("dostrip", true);
		boolean docompile = optmap.getBoolean("docompile", true);
		boolean dodomain = optmap.getBoolean("dodomain", true);
		
		StagingInfoManager sim = new StagingInfoManager(daycode);
		
		// Do the strip operations every day
		if(dostrip)
			{ sim.runStripOps(); }
		
		if(dodomain)
			{ sim.doDomainCompile(); };
		
		// Only run compile operations on Block Start day
		if(docompile && UserIndexUtil.isBlockStartDay(daycode))
		{			
			sim.runCompileOps();
		}
		
		sim._logMail.send2admin();
		sim._repManager.flushInfo();
		
		sim._mailPack.sendPackage();
	}
	
	public StagingInfoManager(String dc) throws IOException
	{
		_dayCode = dc; 
		_logMail = new DayLogMail(this, _dayCode);
		
		_fSystem = FileSystem.get(new Configuration());
		
		_logMail.pf("Staging Process started for %s\n", _dayCode);
		
		_mailPack = new MailPackage();
	}
	
	
	private void runCompileOps() throws IOException
	{
		_logMail.pf("Running Compile Operations for %s\n", _dayCode);
		
		runPixelCompileOp();
		
		runParamPixCompileOp();
		
		runSpecialCompileOp();
		
		runNegativeCompileOp();
                
		writeManifestFile();
	}
	
	private void runPixelCompileOp() throws IOException
	{
		// Sort by nickname
		Map<PosRequest, Set<Integer>> pix2pixlist = Util.treemap();
			
		for(PosRequest preq : ListInfoManager.getSing().getPosRequestSet())
		{
			if(!preq.isActiveOn(_dayCode))
				{ continue; }
						
			if(preq instanceof MultiPixRequest)
			{
				MultiPixRequest multi = (MultiPixRequest) preq;
				Util.setdefault(pix2pixlist, preq, new TreeSet<Integer>());
				pix2pixlist.get(preq).addAll(multi.getPixSet());
			}
		}
				
		_logMail.pf("Running pixel compilation for %d ACTIVE pixel list codes\n", pix2pixlist.size());
				
		for(Map.Entry<PosRequest, Set<Integer>> onepair : pix2pixlist.entrySet())
		{
			// if(!_mailPack.isNew(onepair.getKey()))
			//	{ continue; }
			
			List<String> pixidpathlist = Util.vector();
			
			for(int onepixid : onepair.getValue())
				{ pixidpathlist.addAll(getPixelIdPathList(onepixid)); }
			
			compileNWrite(pixidpathlist, onepair.getKey());
			
			/*
			if(_mailPack.getNumOkay() > 2)
			{
				break;
			}
			*/
		}
		_logMail.pf("Finished with pixel compile operation");
	}
	
	private void runParamPixCompileOp() throws IOException
	{
		SortedSet<Pair<String, PxprmRequest>> lcodeset = Util.treeset();
		
		for(PosRequest preq : ListInfoManager.getSing().getPosRequestSet())
		{
			if(!(preq instanceof PxprmRequest))
				{ continue; }
			
			// Skip inactive lists
			if(!preq.isActiveOn(_dayCode))
				{ continue; }
						
			PxprmRequest pxprmreq = (PxprmRequest) preq;
			
			String nickname = preq.getNickName();
			lcodeset.add(Pair.build(nickname, pxprmreq));
		}		
		
		_logMail.pf("Running PIX PARAM compilation for %d ACTIVE pixel list codes\n", lcodeset.size());
		
		for(Pair<String, PxprmRequest> onepair : lcodeset)
		{
			PxprmRequest ppreq = onepair._2;
			writeIdSet(ppreq.grabIdSet(), ppreq, "PXPRM:: " + ppreq.getKeyValString());
			
			// break;
		}
		_logMail.pf("Finished with param pixel compile operation");
	}	
	
	
	// This should just be a compilation from local to staging, using the standardi compileNWrite operation
	private void runNegativeCompileOp() throws IOException
	{
		for(CountryCode onecty : UserIndexUtil.COUNTRY_CODES)
		{
			List<Path> pathlist = Util.vector();
			Set<String> bigset = Util.treeset();
			NegRequest negreq = ScanRequest.getNegReqMap().get(onecty);
			
			for(String oneday : TimeUtil.getDateRange(_dayCode, UserIndexUtil.PIXEL_FIRE_LOOKBACK_DAYS))
			{
				Path p = new Path(UserIndexUtil.getNegPoolPath(oneday, onecty));
				
				// Path p = new Path(Util.sprintf("/userindex/negpools/%s/pool_%s.txt", oneday, onecty.toString()));
				
				if(_fSystem.exists(p))
				{
					List<String> neglines = HadoopUtil.readFileLinesE(_fSystem, p);
					Set<String> negidset = Util.treeset();
					int lcount = 0;
					for(String oneline : neglines)
					{
						String[] wtpid_hc = oneline.split("\t");
						negidset.add(wtpid_hc[0]);
						lcount++;
					}
					
					// Util.pf("Wrote %d lines for day=%s, cty=%s\n", lcount, oneday, onecty);
					int prevsize = bigset.size();
					bigset.addAll(negidset);
					// _logMail.pf("Added %d ids to bigset from daycode %s, prev size was %d, new size is %d\n", 
					//	negidset.size(), oneday, prevsize, bigset.size());
				}
			}		
			
			
			_logMail.pf("Going to write %d IDs for country %s\n", bigset.size(), onecty);
			
			String negstagingpath = UserIndexUtil.getStagingInfoPath(negreq, _dayCode);
			HadoopUtil.writeLinesToPath(bigset, _fSystem, negstagingpath);
			
			_listCountMap.put(negreq.getListCode(), bigset.size());
		}
	}
	
	private void runSpecialCompileOp() throws IOException
	{
		// Compile the special-PCC list
		for(CountryCode ccode : UserIndexUtil.COUNTRY_CODES)
		{
			List<String> pathidlist = Util.vector();
			for(String oneday : TimeUtil.getDateRange(UserIndexUtil.PIXEL_FIRE_LOOKBACK_DAYS))
			{
				String pccpath = UserIndexUtil.getPccListPath(oneday, ccode.toString());
				pathidlist.add(pccpath);
			}
			
			String listcode = Util.sprintf("%s_%s", StagingType.specpcc, ccode);
			ScanRequest scanreq = ListInfoManager.getSing().getPosRequest(listcode);
			
			compileNWrite(pathidlist, scanreq);	
		}	
	}
	
	void writeManifestFile() throws IOException
	{
		Path manipath = new Path(UserIndexUtil.getStagingManifestPath(_dayCode));
		
		PrintWriter pwrite = HadoopUtil.getHdfsWriter(_fSystem, manipath);
		for(String listcode : _listCountMap.keySet())
		{
			FileUtils.writeRow(pwrite, "\t", listcode, _listCountMap.get(listcode));
		}
		pwrite.close();
		_logMail.pf("Wrote manifest file %s\n", manipath);
		
	}
	
	private void runStripOps() throws IOException
	{
		runPixelStrip();
		
		runSpecialStrip();
	}
	
	private void runSpecialStrip() throws IOException
	{
		_logMail.pf("Running PCC list generator for %s\n", _dayCode);
		PccListGenerator plg = new PccListGenerator(_dayCode);
		plg.loadFromNfs();
		
		for(CountryCode ctry : UserIndexUtil.COUNTRY_CODES)
		{ 
			_logMail.pf("Found %d PCCs for country=%s\n", 
				plg.getUserCount4Ctry(ctry.toString()), ctry);
		}
		
		plg.writeStripData();
		_logMail.pf("Done with PCC list stripping.\n");	
	}
	
	private void runPixelStrip() throws IOException
	{
		List<String> pathlist = Util.getNfsPixelLogPaths(_dayCode);
		Collections.shuffle(pathlist);
		
		DayPixBag dpb = new DayPixBag();
		dpb.cleanLocalDir();
		
		double startup = Util.curtime();
		for(int i = 0; i < pathlist.size(); i++)
		{
			String onepath = pathlist.get(i);
			dpb.processFile(onepath);
			
			if((i % 50) == 0)
				{ _logMail.pf("Finished with file %d/%d, average is %.03f\n", i, pathlist.size(), (Util.curtime()-startup)/(1000*(i+1)));}
		}
		
		// Final flush
		dpb.flush();		
	}		
	
	List<String> getPixelIdPathList(int pixid)
	{
		return getPixelIdPathList(_dayCode, pixid);
	}
	
	static List<String> getPixelIdPathList(String daycode, int pixid)
	{
		List<String> daylist = TimeUtil.getDateRange(daycode, UserIndexUtil.PIXEL_FIRE_LOOKBACK_DAYS);
		List<String> pathlist = Util.vector();
		
		for(String oneday : daylist)
		{
			String strippath = UserIndexUtil.getLocalPixelStripPath(oneday, pixid);
			pathlist.add(strippath);
		}
		
		return pathlist;
	}
	
	
	// Reads WTP ids from a list of paths, writes to the given writer form <ID, listcode> pairs
	void compileNWrite(List<String> idlistpaths, ScanRequest scanreq) throws IOException
	{
		SortedSet<WtpId> idset = Util.treeset();
		int pathcount = 0;
		
		_mailPack.showRequestInfo(scanreq);
		
		for(String onepath : idlistpaths)
		{
			// This might happen if the pixel has only started recently
			if(!(new File(onepath)).exists())
				{ continue; }
			
			List<String> idlines = FileUtils.readFileLinesE(onepath);		
			Util.pf(".");
			for(String oneid : idlines)
			{
				WtpId wid = WtpId.getOrNull(oneid.trim());
				if(wid != null)
					{ idset.add(wid); }
			}
			
			pathcount++;
		}
		
		String extrainfo = Util.sprintf(" checked %d files ", pathcount);
		
		writeIdSet(idset, scanreq, extrainfo);
	}	
	
	void writeIdSet(Set<WtpId> idset, ScanRequest scanreq, String extrainfo) throws IOException
	{
		if(idset.size() < UserIndexUtil.MIN_USER_CUTOFF)
		{
			// Mailpack writes to the main logmail also
			_mailPack.pf(scanreq, "TOO FEW USERS found for %s :: %s, only %d uniques found, %s, skipping\n", 
				scanreq.getListCode(), scanreq.getNickName(), idset.size(), extrainfo);
	
			_repManager.reportStagingCountFail(scanreq, idset.size(), extrainfo, _dayCode, _logMail);
			
			return;
		}
		
		// Util.pf(" done reading, now writing\n");
		String staginginfopath = UserIndexUtil.getStagingInfoPath(scanreq, _dayCode);
		HadoopUtil.writeObjectStr2Path(idset, _fSystem, staginginfopath);
			
		Util.putNoDup(_listCountMap, scanreq.getListCode(), idset.size());
		
		// This writes to the main logmail also
		_mailPack.pf(scanreq, "Found %d total ids for listcode %s :: %s, %s \n", 
			idset.size(), scanreq.getListCode(), scanreq.getNickName(), extrainfo);	
		
		_repManager.reportStagingCountOkay(scanreq.getListCode(), idset.size(), extrainfo, _dayCode, _logMail);	
	}
	
	
	// Returns a map of <specialcode, HdfsPath> for "special" list requests.
	Map<String, Path> getSpecialPathMap() throws IOException
	{
		Map<String, Path> specmap = Util.treemap();
		String pathpatt = Util.sprintf("/userindex/special/*.txt");
		List<Path> speclist = HadoopUtil.getGlobPathList(_fSystem, pathpatt);
		
		for(Path p : speclist)
		{
			String basicname = p.getName();		
			int txtind = basicname.indexOf(".txt");
			specmap.put(basicname.substring(0, txtind), p);
		}
		return specmap;
	}	
	
	void doDomainCompile()
	{
		try { doDomainCompile(_dayCode, _logMail); }
		catch (Exception ex) {
			
			_logMail.addExceptionData(ex);	
		}
	}
	
	static void doDomainCompile(String daycode, SimpleMail logmail)
	{
		
		for(CountryCode onecc : UserIndexUtil.COUNTRY_CODES)
		{			
			String sql = "SELECT FD.id_domain, sum(num_impressions) as impcount " + 
			" FROM fast_domain FD left join cat_country CC on FD.id_country = CC.id " +
			Util.sprintf(" WHERE CC.code = '%s' AND id_date = '%s' GROUP BY FD.id_domain ORDER BY impcount desc LIMIT 5000", 
				onecc.toString().toUpperCase(), daycode);
			
			// String sql = "SELECT * FROM cat_country";
			
			QueryCollector qcol = new QueryCollector(sql, new DatabaseBridge(DbTarget.internal));
			
			// for(int i = 0; i < qcol.getNumRec(); i++)
			// { logmail.pf("Row is %s\n", qcol.getRow(i));	}
		
			String domainpath = UserIndexUtil.getTopDomainPath(onecc, daycode);
			//String domainfile = Util.sprintf("/local/fellowship/userindex/domaincount/dc_%s_%s.txt", onecc, daycode);
			
			FileUtils.createDirForPath(domainpath);
			
			qcol.writeResultE(new File(domainpath));
			
			logmail.pf("Wrote %d records to log path %s for CCode %s\n", qcol.getNumRec(), domainpath, onecc.toString());
		}
					
		// String sql = "SELECT FD.id_domain, count(*) as impcount FROM fast_domain FD left join cat_country CC on FD.id_country = CC.id 
		
	}

	
	public class DayPixBag
	{
		public static final int COMFORT_SIZE = 1000000;
		
		Map<Integer, SortedSet<WtpId>> _batchData = Util.treemap();
		
		// private int _userCount = 0;
		// SortedSet<Pair<Integer, WtpId>> batchData = Util.treeset();
		
		public DayPixBag()
		{
			FileUtils.createDirForPath(UserIndexUtil.getLocalPixelStripPath(_dayCode, 0));
		}
		
		void cleanLocalDir()
		{
			File localdir = new File(UserIndexUtil.getLocalPixelStripDir(_dayCode));
			int delcount = 0;
			
			for(File stripfile : localdir.listFiles())
			{
				// Util.pf("Going to delete strip file %s\n", stripfile.getName());			
				stripfile.delete();
				delcount++;
			}
			
			_logMail.pf("Deleted %d previous local files\n", delcount);
		}
		
		public void processFile(String pixlogpath) throws IOException
		{	
			// Util.pf("Processing file %s\n", pixlogpath);
			
			BufferedReader bread = Util.getGzipReader(pixlogpath);
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				PixelLogEntry ple = new PixelLogEntry(oneline);
				
				int pixid = ple.getIntField(LogField.pixel_id);
				String wtpid = ple.getField(LogField.wtp_user_id);
				
				WtpId wid = WtpId.getOrNull(wtpid);
				if(wid != null)
				{
					Util.setdefault(_batchData, pixid, new TreeSet<WtpId>());
					_batchData.get(pixid).add(wid);
				}
			}
			bread.close();
			
			flushIfNecessary();
		}
		
		void flushIfNecessary() throws IOException
		{
			if(getUserCount() < COMFORT_SIZE)
				{ return; }
			
			flush();
		}
		
		private int getUserCount()
		{
			int t = 0;
			for(SortedSet<WtpId> oneset : _batchData.values())
			{
				t += oneset.size();
			}
			return t;
		}
		
		void flush() throws IOException
		{
			int ucount = getUserCount();			
			int wrotecount = 0;
			// _logMail.pf("Flushing PixData, user count is %d\n", _userCount);
			
			for(Integer newpix : _batchData.keySet())
			{
				// Open a new writer in append mode Append=true
				File pixstrip = new File(UserIndexUtil.getLocalPixelStripPath(_dayCode, newpix));
				BufferedWriter appwrite = new BufferedWriter(new FileWriter(pixstrip, true));
				
				for(WtpId wid : _batchData.get(newpix))
				{
					appwrite.write(wid.toString());
					appwrite.write("\n");
					wrotecount++;
				}
				
				appwrite.close();
			}
			
			_batchData.clear();		
			Util.massert(wrotecount == ucount, "Expected to write %d IDs but only wrote %d", ucount, wrotecount);
		}
	}	
	
	public static class UniqCountReporter
	{
		private String _dayCode;
		
		private static final Pattern PIXID_PATT = Pattern.compile("[\\d]+");
		
		private SortedMap<Integer, Set<String>> _fileMap = Util.treemap();
		
		private static final int MAX_COUNT_CUTOFF = 20000;
		
		public UniqCountReporter(String dc)
		{
			Util.massert(TimeUtil.checkDayCode(dc), "Invalid day code %s", dc);
			
			_dayCode = dc;
			
			
		}
		
		private void popFileMap(File stripfile)
		{
			String sname = stripfile.getName();
			
			Matcher mymatch = PIXID_PATT.matcher(sname);
			boolean found = mymatch.find();			
			
			Util.massert(found && sname.endsWith("strip") && sname.startsWith("pixel_"),
				"Bad strip file name %s", stripfile);
			
			int pixid = Integer.valueOf(mymatch.group());
			
			Util.setdefault(_fileMap, pixid, new TreeSet<String>());
			_fileMap.get(pixid).add(stripfile.getAbsolutePath());			
		}
		
		public void generateFileMap()
		{
			for(int n = 0; n < 30; n++)
			{
				String prevday = TimeUtil.nDaysBefore(_dayCode, n);
				Util.pf("Prev day is %s\n", prevday);
				File stripdir = new File(UserIndexUtil.getLocalPixelStripDir(prevday));
				Util.massert(stripdir.exists() && stripdir.isDirectory(),
					"Problem with pixel strip dir %s", stripdir);
				
				for(File onestrip : stripdir.listFiles())
				{
					// Util.pf("One strip is %s\n", onestrip.getAbsolutePath());
					popFileMap(onestrip);					
				}
			}
		}
		
		public void runUniqueCounts()
		{
			double startup = Util.curtime();
			int checkcount = 0;
			
			for(int pixid : _fileMap.keySet())
			{
				int ucount = countUniq4Pix(pixid);
				
				AdBoardApi.reportUniqPixelCount(pixid, ucount);
				
				checkcount++;
				
				if((checkcount % 20) == 0)
				{
					Util.pf("Finished counting %d out of %d pixels, estcomplete=%s\n",
						checkcount, _fileMap.size(), 
						TimeUtil.getEstCompletedTime(checkcount, _fileMap.size(), startup));
				}
			}
		}
		
		public int countUniq4Pix(int pixid)
		{
			Util.massert(_fileMap.containsKey(pixid), "Pixel ID %d not found in file map");
			
			Set<WtpId> oneset = Util.treeset();
			int fcount = 0;
			
			for(String onepath : _fileMap.get(pixid))
			{
				for(String oneid : FileUtils.readFileLinesE(onepath))
				{
					WtpId wid = WtpId.getOrNull(oneid);
					Util.massert(wid != null, "Bad WTP found in strip file %s", oneid);
					oneset.add(wid);
				}
				fcount++;
				
				// Util.pf("Uniq count is now %d after reading path %s\n",
				// 	oneset.size(), onepath);
				
				if(oneset.size() > MAX_COUNT_CUTOFF)
					{ break; }
			}
			
			// Util.pf("Found %d uniques for pixid %d after checking %d files \n", 
			//	oneset.size(), pixid, fcount);
			
			return oneset.size();
		}
	}
	
	private class PccListGenerator
	{		
		Map<String, SortedSet<String>> _idCtryMap = Util.treemap();
		private String _dayCode;
		
		PccListGenerator(String dc)
		{
			_dayCode = dc;	
		}
		
		public int getUserCount4Ctry(String ctry)
		{
			return _idCtryMap.containsKey(ctry) ? _idCtryMap.get(ctry).size() : 0;
		}
		
		private void writeStripData()
		{
			for(String ccode : _idCtryMap.keySet())
			{
				String writepath = UserIndexUtil.getPccListPath(_dayCode, ccode);
				FileUtils.createDirForPath(writepath);
				FileUtils.writeFileLinesE(_idCtryMap.get(ccode), writepath);
			}
		}
		
		private void loadFromNfs() throws IOException
		{
			List<String> convlist = Util.vector();
			
			for(ExcName oneexc : ExcName.values())
			{
				List<String> onelist = Util.getNfsLogPaths(oneexc, LogType.conversion, _dayCode);
				if(onelist != null)
					{ convlist.addAll(onelist); }
			}
			
			for(String onepath : convlist)
			{
				Util.pf(".");
				// Util.pf("Reading from path %s\n", onepath);
				
				PathInfo pinfo = new PathInfo(onepath);
				
				try {
					BufferedReader bread = Util.getGzipReader(onepath);
					for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
					{
						BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
						if(ble == null)
							{ continue; }
						
						if(ble.getIntField(LogField.is_post_click) == 0) // only interested in post_click
							{ continue; }
						
						String ctry = ble.getField(LogField.user_country).trim();
						if(ctry.length() == 0)
							{ continue; }
						
						WtpId wid = WtpId.getOrNull(ble.getField(LogField.wtp_user_id));
						if(wid != null)
						{	
							Util.setdefault(_idCtryMap, ctry, new TreeSet<String>());
							_idCtryMap.get(ctry).add(wid.toString());
						}
					}
					bread.close();
				} catch (IOException ioex) {
					
					Util.pf("Hit exception on file %s\n", onepath);
				}
			}
		}
	}	
	
	private class MailPackage
	{
		// Requester email :: mail object
		private SortedMap<String, SimpleMail> _mailPack = Util.treemap();
		
		private SortedMap<String, Integer> _countMap = Util.treemap();
		
		private Set<String> _okaySet = Util.treeset();
		
		

		MailPackage()
		{
			_okaySet.add("daniel.burfoot@digilant.com");
			_okaySet.add("daniel.davies@adnetik.com");
			_okaySet.add("raz@adnetik.com");
		}
		
		private void pvInitMail(PosRequest posreq)
		{
			String reqemail = posreq.getRequester();
			
			if(!_mailPack.containsKey(reqemail))
			{
				// these guys don't use timestamps
				SimpleMail onemail = new SimpleMail("AIDX StagingInfo Report for " + _dayCode, false);
				_mailPack.put(reqemail, onemail);				
			}
		}
		
		void showRequestInfo(ScanRequest scanreq)
		{
			if(scanreq instanceof PosRequest)
			{
				PosRequest posreq = (PosRequest) scanreq;
				pvInitMail(posreq);
				
				pf(posreq, "Info for AIDX request %s:\n", posreq.getListCode());
				pf(posreq, "BasicInfo: %s\n", posreq.getBasicInfo());
				pf(posreq, "Name: %s\n", posreq.getNickName());
				pf(posreq, "Country: %s, Expires: %s\n", 
					posreq.getCountryCode(), posreq.getExpirationDate());	
			}
		}
		
		void pf(ScanRequest scanreq, String formstr, Object... varargs)
		{
			if(scanreq instanceof PosRequest)
			{
				PosRequest preq = (PosRequest) scanreq;
				pvInitMail(preq);
			
				Util.massert(_mailPack.containsKey(preq.getRequester()), 
					"Could not find requester email %s in mailpack", preq.getRequester());
				
				// Write to the user email
				_mailPack.get(preq.getRequester()).pf(formstr, varargs);
				
				// Keep track of how many lines are written to each email, don't send empty mails
				Util.incHitMap(_countMap, preq.getRequester());
			}			
			
			// Write to the base email
			_logMail.pf(formstr, varargs);
		}
		
		void sendPackage()
		{
			for(String keyaddr : _mailPack.keySet())
			{
				SimpleMail onemail = _mailPack.get(keyaddr);
				
				// if(!_okaySet.contains(keyaddr))
				//	{ continue; }
				
				// If the countmap has any values, it means its non-zero
				if(!_countMap.containsKey(keyaddr))
					{ continue; }
				
				// send to actual recp
				onemail.send(keyaddr);
				
				// also send to admin
				// conemail.pf("This report was sent to %s\n", keyaddr);
				// onemail.send2admin();
			}
		}
		
		int getNumOkay()
		{
			int nokay = 0;
			
			for(String keyaddr : _mailPack.keySet())
			{
				if(_okaySet.contains(keyaddr))
					{ nokay++; }
			}
			
			return nokay;
		}
		
		private boolean isNew(PosRequest preq)
		{
			if(!_mailPack.containsKey(preq.getRequester()))
			{
				Util.pf("Found new requster email %s\n", preq.getRequester());	
				return true;
			}
			
			return false;
		}
	}
}
