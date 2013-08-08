
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.data_management.SegmentPathMan.*;

// This a combined data man for several snapshot files: IAB, IP, and Domain.
public class BidLogDataMan extends DailyDaemon
{
	private String _dayCode;
	private Double _pathFrac; // percentage of paths to use

	private BufferedWriter _iabWriter;
	private BufferedWriter _ippWriter;
	
	private List<String> _pathList = Util.vector();
	
	boolean _doPreProc;
	boolean _doMerge;
	
	private SimpleMail _logMail;
	
	public static void main(String[] args) throws IOException
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		ArgMap argmap = Util.getClArgMap(args);

		BidLogDataMan bldm = new BidLogDataMan(TimeUtil.dayAfter(daycode), argmap);
		bldm.startDaemon();
	}
	
	public BidLogDataMan(String dc, ArgMap argmap)
	{
		super(dc);
		
		_pathFrac = argmap.getDouble("pathfrac", 0.05D);
		_doPreProc = argmap.getBoolean("dopreproc", true);
		_doMerge = argmap.getBoolean("domerge", true);		
	}		
	
	public void runProcess()
	{
		_dayCode = getPrevDayCode();
		_logMail = new DayLogMail(this, _dayCode);
		
		try { 
			if(_doPreProc)
			{
				initPathList();
				preprocData();
			} 
			
			if(_doMerge)
			{			
				doMergeOps();	
			}			
		} catch (Exception ex) {
			
			_logMail.addExceptionData(ex);	
		}
		
		_logMail.send2admin();
	}
	
	void initPathList()
	{
		LinkedList<String> gimplist = Util.linkedlist();
		
		for(ExcName oneexc : ExcName.values())
		{
			List<String> plist = Util.getNfsLogPaths(oneexc, LogType.bid_all, _dayCode);
			if(plist == null)
				{ continue; }
			
			gimplist.addAll(plist);
		}
		
		double maxfile = _pathFrac;
		maxfile *= gimplist.size();
		
		Collections.shuffle(gimplist);
		
		// Gotcha, using this thing in daemon mode, but was keeping around 
		// old pathlist data. 
		_pathList.clear();				
		
		while(_pathList.size() < maxfile && !gimplist.isEmpty())
			{ _pathList.add(gimplist.poll()); }
		
		Util.pf("Found %d paths total, pathfrac=%.03f gives %d to use\n",
			gimplist.size(), _pathFrac, _pathList.size());
	}
	
	@Override
	public String getShortStartTimeStamp()
	{
		return "06:00:00"; 
	}
	
	void preprocData() throws IOException
	{
		_iabWriter = FileUtils.getWriter(Party3Type.iab_ourdata.getProcFilePath(_dayCode));
		_ippWriter = FileUtils.getWriter(Party3Type.ipp_ourdata.getProcFilePath(_dayCode));
		
		for(int i : Util.range(_pathList.size()))
		{
			String onepath = _pathList.get(i);			

			try { preprocFile(onepath); }
			catch (IOException ioex) {
				
				// TODO: catch io exceptions	
			}
			
			if(((i+1) % 10) == 0)
			{
				_logMail.pf("Finished processing file %d / %d\n", i, _pathList.size());
			}
		}
		
		_ippWriter.close();
		_iabWriter.close();
	}
	
	void preprocFile(String onepath) throws IOException
	{
		PathInfo pinfo = new PathInfo(onepath);
		BufferedReader bread = FileUtils.getGzipReader(onepath);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			BidLogEntry ble = BidLogEntry.getOrNull(pinfo.pType, pinfo.pVers, oneline);
			if(ble == null)
				{ continue; }
			
			String wtpid = ble.getField(LogField.wtp_user_id);
			WtpId wid = WtpId.getOrNull(wtpid);
			if(wid == null)
				{ continue; }
			
			{
				try { 
					Set<Integer> iabset = ble.getIabSegSet();
					if(!iabset.isEmpty())
					{	
						String segstr = Util.join(iabset, ",");
						// Util.pf("IAB seg info is %s\n", segstr);
						FileUtils.writeRow(_iabWriter, "\t", wtpid, _dayCode, segstr);
					}
				} catch (BidLogFormatException blex) {
					;; // Do nothing	
				}
				
			} {
				String userip = ble.getField(LogField.user_ip);			
				FileUtils.writeRow(_ippWriter, "\t", wtpid, _dayCode, userip);
			}
		}
		
		bread.close();
	}
	
	void doMergeOps() throws IOException
	{
		List<SegmentDataQueue> sqlist = Util.vector();
		
		sqlist.add(new BidLogQ.IppQueue());
		sqlist.add(new BidLogQ.IabQueue());
		
		for(SegmentDataQueue oneq : sqlist)
		{
			// Read from the local disk
			oneq.initFromLocal(TimeUtil.dayBefore(_dayCode));
			
			SnapshotMergeOp smo = new SnapshotMergeOp(oneq, _dayCode, _logMail);
			
			smo.buildSortMap();
			smo.merge();
			smo.finishUp(3);			
		}
	}
}