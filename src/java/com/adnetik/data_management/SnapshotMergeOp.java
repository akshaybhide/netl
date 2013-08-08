
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;           
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

// This is the generic code related to a merge operation
public class SnapshotMergeOp
{
	protected String _dayCode;
	
	private SortedFileMap _sortFileMap;
		
	protected SimpleMail _logMail;
	
	private int _slurpLines = 0;
	private int _newUsers = 0;
	private int _newTotal = 0;				
	private int _updateUsers = 0;
	private int _noUpdateUsers = 0;
	private int _badWtpIds = 0;

	SegmentDataQueue _segDataQ;
	
	public SnapshotMergeOp(SegmentDataQueue srcq, String daycode, SimpleMail logmail)
	{
		TimeUtil.assertValidDayCode(daycode);
		
		_segDataQ = srcq;
		
		_logMail = logmail;
		
		_dayCode = daycode;
	}
	
	void buildSortMap() throws IOException
	{
		String preprocfile = _segDataQ.getDataSet().getProcFilePath(_dayCode);
		
		// Going to do the sort myself, instead of using SortMapFile
		_logMail.pf("Preparing to sort slurpfile...");
		Util.unixsort(preprocfile, "");
			
		// dosort=false
		_sortFileMap = SortedFileMap.buildFromFile(new File(preprocfile), false);
	}
	
	// We need this check here, annoyingly, because the SegmentPack has a type parameter,
	// but we don't know the type parameter here.
	@SuppressWarnings("unchecked")	
	void merge() throws IOException
	{
		double startup = Util.curtime();
		_logMail.pf("Starting merge operation for dataset %s\n", _segDataQ.getDataSet());
		String dropcutoff = TimeUtil.nDaysBefore(_dayCode, 60);
		
		BufferedWriter pwrite = _segDataQ.getPathMan().getGimpWriter(_dayCode);
		
		int writecount = 0;
		
		while(_segDataQ.hasNext())
		{
			SegmentPack bupnext = _segDataQ.nextPack();
			
			while(!_sortFileMap.isEmpty() && _sortFileMap.firstKey().compareTo(bupnext.wtpid) < 0)
			{
				// This is a new WTP id that is not in the Master file, and was found for the first time today
				Map.Entry<String, List<String>> ent = _sortFileMap.pollFirstEntry();
				
				SegmentPack bupnew = _segDataQ.buildEmpty(ent.getKey());
				bupnew.integrateNewData(ent.getValue(), _dayCode);
				writecount += bupnew.write(pwrite, dropcutoff);
				_newUsers++;
				_newTotal++;
			}
			
			if(!_sortFileMap.isEmpty() && _sortFileMap.firstKey().equals(bupnext.wtpid))
			{
				// Here there is data in both the Master file and the day's slurped data
				Map.Entry<String, List<String>> ent = _sortFileMap.pollFirstEntry();
				bupnext.integrateNewData(ent.getValue(), _dayCode);
				_updateUsers++;
				
			} else {
				_noUpdateUsers++;	
			}
			
			_newTotal++;
			writecount += bupnext.write(pwrite, dropcutoff);
			
			if((_segDataQ.getPollUserCount() % 10000) == 0)
			{
				Util.pf(".");
				//double userpersec = bqueue.polledUsers /((Util.curtime()-startup)/1000);
				//Util.pf("Finished with %d users, %.03f users per second\n",
				//	bqueue.polledUsers, userpersec);
			}
		}
		
		Util.pf("\n");
		
		while(!_sortFileMap.isEmpty())
		{
			// Write out remaining data.
			Map.Entry<String, List<String>> ent = _sortFileMap.pollFirstEntry();
			
			SegmentPack bupnew = _segDataQ.buildEmpty(ent.getKey());
			bupnew.integrateNewData(ent.getValue(), _dayCode);			
					
			// TODO: this doesn't inform us about whether or not a user actually has any data associated with him
			writecount += bupnew.write(pwrite, dropcutoff);	
			_newTotal++;				
		}
		
		pwrite.close();
		
		// TODO: put these stats back in
		_logMail.pf("Finished merge, stats: \n\t%d updated users\n\t%d non-updated\n\t%d prev total\n\t%d new total\n",
			_updateUsers, _noUpdateUsers, _segDataQ.polledUsers, _newTotal);
		
		_logMail.pf("Master file size: \n%d lines before\n%d lines after\n", _segDataQ.linesRead, writecount);
		
	}			

	
	void finishUp(int savelocal) throws IOException
	{
		SegmentPathMan spman = _segDataQ.getPathMan();
		
		spman.renameGimp2Master(_dayCode);
		spman.uploadMaster(_dayCode, _logMail);
		spman.deleteOldLocalMaster(_dayCode, savelocal, _logMail);
		
		// Delete the temp file
		{
			File preprocfile = new File(_segDataQ.getDataSet().getProcFilePath(_dayCode));	
			
			if(preprocfile.exists())
			{  
				preprocfile.delete(); 
				_logMail.pf("Deleted slurp file %s\n", preprocfile.getAbsolutePath());
			}
		}
	}	
	
	
}
