
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
public class PixDataMan extends DailyDaemon
{
	private String _dayCode;

	private BufferedWriter _pixWriter;
	
	private List<String> _pathList = Util.vector();
	
	boolean _doPreProc;
	boolean _doMerge;
	
	private double _pathFrac = 0.1D;
	
	private SimpleMail _logMail;
	
	public static void main(String[] args) throws IOException
	{
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		ArgMap argmap = Util.getClArgMap(args);

		PixDataMan pdm = new PixDataMan(daycode, argmap);
		pdm.startDaemon();
	}
	
	public PixDataMan(String dc, ArgMap argmap)
	{
		super(dc);
		
		_pathFrac = argmap.getDouble("pathfrac", _pathFrac);
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
			List<String> plist = Util.getNfsPixelLogPaths(_dayCode);
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
		return "14:00:00"; 
	}
	
	void preprocData() throws IOException
	{
		_pixWriter = FileUtils.getWriter(Party3Type.pix_ourdata.getProcFilePath(_dayCode));
		
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
		
		_pixWriter.close();
	}
	
	void preprocFile(String onepath) throws IOException
	{
		BufferedReader bread = FileUtils.getGzipReader(onepath);
		
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{
			PixelLogEntry ple = PixelLogEntry.getOrNull(oneline);
			if(ple == null)
				{ continue; }
			
			String wtpid = ple.getField(LogField.wtp_user_id);
			WtpId wid = WtpId.getOrNull(wtpid);
			if(wid == null)
				{ continue; }
			
			int pixid = ple.getIntField(LogField.pixel_id);
			
			FileUtils.writeRow(_pixWriter, "\t", wtpid, _dayCode, pixid);
		}
		
		bread.close();
	}
	
	void doMergeOps() throws IOException
	{
		List<SegmentDataQueue> sqlist = Util.vector();
		sqlist.add(new BidLogQ.PixQueue());
		
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
