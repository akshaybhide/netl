
package com.adnetik.userindex;

import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.userindex.UserIndexUtil.*;
import com.adnetik.userindex.ScanRequest.*;


// Handles the work of generating a feature report and learning a classifier.
public class LearningTool extends Configured implements Tool
{	
	// private Map<String, PrecompFeatPack> _precMap = Util.treemap();
	
	private Map<String, String> _pos2negMap = Util.treemap();
	
	Map<PosRequest, AdaBoost<UserPack>> pos2classMap = Util.conchashmap();
	
	// ListCode-> writer
	Map<String, List<String>> learnLogMap = Util.treemap();
	
	String _blockEnd;
	
	Integer maxUserCount;
	Set<String> _learnTargSet;
	
	Set<ScanRequest> _precReadySet = Util.treeset();
	
	SimpleMail _logMail;
	
	StatusReportMan _repManager = new StatusReportMan();
	
	// paranoid error-checking
	private Set<ScanRequest> _loadedSet = Util.treeset();
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);
	}
	
	// TODO: why is this a Hadoop Tool and not a Java standalone? It does not need to be a Java
	// standalone.
	public int run(String[] args) throws Exception
	{
		ArgMap amap = Util.getClArgMap(args);
		_blockEnd = amap.getDayCode("blockend", TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay()));
		UserIndexUtil.assertValidBlockEnd(_blockEnd);
		
		_logMail = new SimpleMail(UserIndexUtil.AIDX_WEEKLY_CODE + " LearningTool for " + _blockEnd);
		
		_logMail.pf("Running learning process for daycode %s\n", _blockEnd);
		
		// 		
		UFeatManager.initSing(_blockEnd);
		
		maxUserCount = amap.getInt("maxuser", UserIndexUtil.MAX_USERS_LEARN);

		// Get rid of list codes that aren't ready
		findPrecReady();
	
		// If learntarg is non-null, drop codes other than those in learn
		String learntarg = amap.getString("learntarg", null);
		dropNonLearnTarg(learntarg);		
		
		_logMail.pf("Going to learn for listcodes %s\n", _pos2negMap.keySet());
		
		// loadPrecFromHdfs();
		// loadPrecFromLocal();
		
		multiThreadLearn();
		
		// NB Don't delete old until you have the new!!!
		saveLearnLogs();
		
		upload2Database();
		
		_logMail.send2admin();
		_repManager.flushInfo();
					
		return 1;
	}
	
	public LearningTool() throws IOException
	{
		_pos2negMap = UserIndexUtil.getPos2NegMap();		
	}
	
	// Drop list codes if the data is not ready
	/*
	void dropNonReady() throws IOException
	{
		CheckReady cready = new CheckReady();
		cready.setDayCode(_blockEnd);
		
		// Need to create new list, then iterate over it, because you cannot remove from map
		// while you're doing the iteration.
		List<String> checklist = new Vector<String>(_pos2negMap.keySet());
		for(String checkone : checklist)
		{
			if(!cready.isListReady(checkone))
			{ 
				_pos2negMap.remove(checkone);

				// Only worth reporting this if the list code is actually active, there will be many
				// inactive list codes, don't need a message for all of them.
				if(ListInfoManager.getSing().isActive(checkone))
				{
					_logMail.pf("Listcode %s is not yet ready to be scored\n", checkone);
					_repManager.sendReport(checkone, ReportType.learn, false, "List code is not ready for learning");
				}
			}
		}
	}
	*/
	
	void findPrecReady() throws IOException
	{
		for(ScanRequest screq : ListInfoManager.getSing().getFullScanReqSet())
		{
			PrecompFeatPack pfp = new PrecompFeatPack(_blockEnd, screq);
			
			if(pfp.precompFileExists())
				{ _precReadySet.add(screq); }
		}
		
		Util.pf("Found %d precomp files ready for blockend %s\n",
			_precReadySet.size(), _blockEnd);
	}
	
	void dropNonLearnTarg(String learntarg)
	{
		// Keep them all		
		if(learntarg == null)
			{ return; }
		
		Set<String> keepset = Util.treeset();
		for(String onetarg : learntarg.split(","))
		{
			Util.massert(_pos2negMap.containsKey(onetarg), "Listcode %s not found in pos2negMap");
			keepset.add(onetarg); 
		}
		
		_logMail.pf("Found learn targ set %s, size %d\n", keepset, keepset.size());		
					
		List<String> checklist = new Vector<String>(_pos2negMap.keySet());
		for(String onecheck : checklist)
		{
			if(!keepset.contains(onecheck))
				{ _pos2negMap.remove(onecheck); }
		}
				
		// Util.pf("Dropped extra learn codes, using only %s\n", pos2negMap.toString());
	}
	
	// Return set of negative list codes to be used for this scan
	public Set<ScanRequest> getNegSet()
	{
		Set<ScanRequest> negset = Util.treeset();
		
		for(String oneneg : _pos2negMap.values())
		{ 
			ScanRequest screq = ListInfoManager.getSing().getRequest(oneneg);
			negset.add(screq); 
		}
		
		return negset;
	}
	
	// Return positive codes associated with this negative list
	// TODO: put in ListInfoManager
	public Set<String> getPos4Neg(String neglist)
	{
		Set<String> pset = Util.treeset();
		for(Map.Entry<String, String> oneentry : _pos2negMap.entrySet())
		{
			if(neglist.equals(oneentry.getValue()))
				{ pset.add(oneentry.getKey()); }
			
		}
		return pset;
	}
	
	private void upload2Database() throws IOException
	{
		UserIdxDb.uploadClassifData(pos2classMap, _blockEnd);
		
		_logMail.pf("Uploaded %d classifiers to DB\n", pos2classMap.size());
	}
	
	
	private PrecompFeatPack loadPrecData(ScanRequest screq) throws IOException
	{
		Util.massert(!_loadedSet.contains(screq), "Error: listcode %s loaded TWICE", screq.getListCode());
		_loadedSet.add(screq);		
		
		_logMail.pf("Loading list code %s\n", screq.getListCode());
		PrecompFeatPack pfp = new PrecompFeatPack(_blockEnd, screq);
		
		if(!pfp.precompFileExists())
			{ return null; }
		
		pfp.loadFromHdfs(maxUserCount);
		return pfp;
	}
	
	void saveLearnLogs()
	{
		for(Map.Entry<String, List<String>> lent : learnLogMap.entrySet())
		{
			String savepath = Util.sprintf("%s/learnlog/%s/%s.log", UserIndexUtil.LOCAL_UINDEX_DIR, _blockEnd, lent.getKey());
			FileUtils.createDirForPath(savepath);
			FileUtils.writeFileLinesE(lent.getValue(), savepath);			
		}
	}
	
	void multiThreadLearn() throws Exception
	{
		Map<String, LearnThread> lmap = Util.treemap();
		int tcount = 0;
		int errcount = 0;
		
		for(ScanRequest negreq : getNegSet())
		{
			String neglist = negreq.getListCode();
			PrecompFeatPack negpack = loadPrecData(negreq);
			int negpackusers = negpack.numUsers();	
			
			// If this happens, it's a real problem
			if(negpackusers < UserIndexUtil.MIN_USER_CUTOFF)
			{ 
				_logMail.pf("WARNING!!! Found only %d users for negative listcode=%s, skipping\n", 
					negpack.numUsers(), neglist);

				_repManager.reportLearningCountFail(neglist, negpack.numUsers(), _blockEnd, _logMail);
				continue;
			}			
			
			for(String poslist : getPos4Neg(neglist))
			{
				PosRequest preq = ListInfoManager.getSing().getPosRequest(poslist);
				
				// No precomp file is ready; skip.
				if(!_precReadySet.contains(preq))
					{ continue; }
				
				PrecompFeatPack pospack = null;
				
				try {  pospack = loadPrecData(preq); }
				catch (Exception ex) 
				{ 
					{
						SimpleMail logmail = new SimpleMail("LearningTool Exception");
						logmail.pf("Problem loading precomp data for poslist %s", poslist);
						logmail.addExceptionData(ex);
						logmail.send2admin();
					}
					
					errcount++;
					
					if(errcount >= 4)
						{ throw new RuntimeException(ex); }
				}
				
				if(pospack == null)
					{ continue; }
				
				if(pospack.numUsers() < UserIndexUtil.MIN_USER_CUTOFF)
				{ 
					_repManager.sendReportPf(poslist, ReportType.learn, false, "Found only %d users, too few for learning", pospack.numUsers());
					
					_logMail.pf("SKIPPING: Found only %d users for positive listcode=%s\n", 
						pospack.numUsers(), poslist);
					
					continue;
				}				
								
				LearnThread lt = new LearnThread(pospack, negpack, preq, tcount++);
				// lt.start();
				lt.run();			
				
				// Maybe the learning could fail...?
				if(pos2classMap.containsKey(preq))
					{ persist(preq); }
			}
		}
	} 
	
	void persist(PosRequest posreq) throws IOException
	{
		FileSystem fsys = FileSystem.get(getConf());
		
		Util.massert(pos2classMap.containsKey(posreq),
			"Persist called for posreq %s but it is not yet ready!", posreq.getListCode());
		
		_logMail.pf("Writing class for list code %s\n", posreq.getListCode());
		
		AdaBoost<UserPack> adaclass = pos2classMap.get(posreq);
		String savepath = UserIndexUtil.getAdaSavePath(posreq.getListCode(), _blockEnd);
		HadoopUtil.writeLinesToPath(adaclass.getSaveListInfo(), fsys, savepath);
		
		_repManager.reportLearningSuccessful(posreq.getListCode(), _blockEnd, _logMail);
	}
	
	synchronized int learnCount()
	{
		return pos2classMap.size();
	}
	
	synchronized void setLearnResult(PosRequest posreq, AdaBoost<UserPack> adaclass)
	{
		pos2classMap.put(posreq, adaclass);
		
	}
	
	synchronized void reportStatus(String funcname, String listcode, double besterr, int numerr, int threadid)
	{
		// _logMail.pf("Thread %d: function %s produced besterr %.03f, %d errors\n", 
		//	threadid, funcname, besterr, numerr);
		
		String logline = Util.sprintf("%s\t%s\t%.03f\t%d\t%d", 
			funcname, listcode, besterr, numerr, threadid);
		
		Util.setdefault(learnLogMap, listcode, new Vector<String>());
		learnLogMap.get(listcode).add(logline);
	}
	
	private class LearnThread extends Thread
	{
		PosRequest _posReq;
		PrecompFeatPack _posPack;
		PrecompFeatPack _negPack;
		
		// UIndexDataManager dataMan;
		AdaBoost<UserPack> adaclass;
		int threadId;
		
		public LearnThread(PrecompFeatPack ppack, PrecompFeatPack npack, PosRequest preq, int tid)
		{
			// dataMan = uidm;
			_posPack = ppack;
			_negPack = npack;
			
			_posReq = preq;
			threadId = tid;
		}
		
		public void run()
		{
			_logMail.pf("Now training for %s\n", _posReq.getListCode());
			doBasicTraining(50);			
			setLearnResult(_posReq, adaclass);
			
			/*
			for(FeatureCode onecode : FeatureCode.values())
			{
				if(onecode.isThirdParty())
				{
					AdaBoost<UserPack> newada = doThirdPartyTraining(onecode, 10);
					setLearnResult(posList + "__" + onecode.toString(), newada);
				}
			}
			*/
		}
		
		void doBasicTraining(int numrounds)
		{
			Set<FeatureCode> featcodeset = ListInfoManager.getSing().getLearnCodes4List(_posReq);
			Util.pf("Feat code set is %s\n", featcodeset);			
			
			// Include featcodeset, means that we will DQ features not in the set
			Map<String, boolean[]> subfeatmap = PrecompFeatPack.combine(_posPack, _negPack, featcodeset);

			boolean[] targvec = PrecompFeatPack.genTargVec(_posPack, _negPack);
			adaclass = new AdaBoost<UserPack>(targvec);
			
			for(int i = 0; i < numrounds; i++)
			{
				double besterr = adaclass.oneRoundPc(subfeatmap);
				
				Util.pf("done with round %d, last best is %s, besterr is %.04f\n",
					i, adaclass.lastBestFunc(), besterr);
				
				int numerr = adaclass.numErrs(subfeatmap, targvec);
				// Util.pf("\nNumber of errors is now %d, rate %.03f", numerr, ((double) numerr)/targVec.length);
				
				reportStatus(adaclass.lastBestFunc(), _posReq.getListCode(), besterr, numerr, threadId);
				
				if(besterr > .495)
					{ break; } 
			}
		}		
		
		/*
		AdaBoost<UserPack> doThirdPartyTraining(FeatureCode party3code, int numrounds)
		{
			Util.massert(party3code.isThirdParty());
			Map<String, boolean[]> featcodemap = dataMan.getOneFeatureCodeMap(party3code);
			Map<String, boolean[]> basicplusmap = dataMan.getBasicPlusFeature(party3code);
			
			AdaBoost<UserPack> newada = Util.serializeCloneE(adaclass);
			
			if(featcodemap.isEmpty())
				{ return newada; } 
			
			for(int i = 0; i < numrounds; i++)
			{
				// Only test using the NEW 3rd party features
				double besterr = adaclass.oneRoundPc(featcodemap);
				
				// But score using the entire feature pack
				int numerr = adaclass.numErrs(basicplusmap, dataMan.getTargVec());
				
				reportStatus(adaclass.lastBestFunc(), posList, besterr, numerr, threadId);
				
				if(besterr > .495)
					{ break; } 
			}
			
			return newada;
		}	
		*/
	}
}
