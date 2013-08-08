
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

import com.adnetik.data_management.SegmentDataQueue.*;
import com.adnetik.data_management.SegmentPathMan.*;

// This class mirrors structure of 
public class ExelateDataMan
{
	// public static final String DATA_MAN_TEMP_FILE = "/tmp/exelate/DATAMAN_TEMP.txt";
	
	private static ExelateUserQueue _SING_QUEUE;
	
	private static TreeMap<String, Map<Integer, String>> _DAY_BEFORE_MAP = Util.treemap();
	
	// dogzip=false
	public static SegmentPathMan EX_PATH_MAN = new SegmentPathMan(SegmentPathMan.Party3Type.exelate, true);
	
	private static int TARG_Q_SIZE = 1000;
	
	public static synchronized ExelateUserQueue getSingQ()
	{
		Util.massert(_SING_QUEUE != null, "Must call initSingQ first");
		return _SING_QUEUE;
	}
	
	public static synchronized boolean isQReady()
	{
		return _SING_QUEUE != null;
	}
	
	public static synchronized void setSingQ(String daycode) throws IOException
	{
		BufferedReader bread = EX_PATH_MAN.getHdfsMasterReader(daycode);
		
		_SING_QUEUE = new ExelateUserQueue(FileUtils.bufRead2Line(bread));
	}
		
	
	public static synchronized void setLocalSingQ(String daycode) throws IOException
	{
		File localmaster = new File(EX_PATH_MAN.getLocalMasterPath(daycode));
		Util.massert(localmaster.exists(), "Master path %s for day %s not ready", localmaster.getAbsolutePath(), daycode);
		
		BufferedReader bread = EX_PATH_MAN.getLocalMasterReader(daycode);
		_SING_QUEUE = new ExelateUserQueue(FileUtils.bufRead2Line(bread));		
	}


	public static void main(String[] args) throws IOException
	{
		if(args.length != 1)
		{
			Util.pf("Usage: ExelateDataMan <yest|daycode>");
			return;
		}
		
		String daycode = "yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0];
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode %s", daycode);
	
		(new ExelMergeDaemon(TimeUtil.dayAfter(daycode))).startDaemon();
	}
	
	public static String getHdfsDumpPath(String dc)
	{
		return Util.sprintf("/thirdparty/exelate/dump/dump_%s.tsv", dc);
	}	
	
	public static String getS3NDumpPath(String dc, int i)
	{
		String dpath = Util.sprintf("/exelate_%s_%s.tsv.gz", 
			TimeUtil.dayCode2Int(dc), Util.padLeadingZeros(i, 2));
		
		return dpath;
	}		
	
	public static class ExelMergeDaemon extends DailyDaemon
	{
		public ExelMergeDaemon(String dc)
		{
			super(dc);	
		}
		
		protected  String getShortStartTimeStamp()
		{ 
			return "17:00:00"; 
		}
		
		public void runProcess()
		{
			try {
				// We're building the snapshot file for this day
				String daycode = getPrevDayCode();
				SimpleMail logmail = new DayLogMail(this, daycode);
				
				// This needs to be done here, so the SegmentMergeOp construction 
				// has the reference to the Q
				// Need prev snapshot for day before
				setLocalSingQ(TimeUtil.dayBefore(daycode));			
				
				ExMergeOp exmerge = new ExMergeOp(ExelateDataMan.getSingQ(), daycode, logmail);
				exmerge.runOp();
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);	
			}
		}
	}	
	
	private static String findDayBefore(int numdays, String daycode)
	{
		
		if(!_DAY_BEFORE_MAP.containsKey(daycode))
		{
			// POSITIVE numbers are in the past, 
			// NEGATIVE numbers are in the future, whatever that means
			Map<Integer, String> singledaymap = Util.treemap();
			
			String daycursor = daycode;
			for(int i = 0; i < 300; i++)
			{
				singledaymap.put(i, daycursor);
				daycursor = TimeUtil.dayBefore(daycursor);
			}
			
			daycursor = daycode;
			for(int i = 0; i < 20; i++)
			{
				singledaymap.put(-i, daycursor);
				daycursor = TimeUtil.dayAfter(daycursor);
			}
			
			_DAY_BEFORE_MAP.put(daycode, singledaymap);
		}
		
		Util.massert(_DAY_BEFORE_MAP.get(daycode).containsKey(numdays), "Numdays arg is out of range %d", numdays);
		
		return _DAY_BEFORE_MAP.get(daycode).get(numdays);
	}	
	
	
	public static class ExMergeOp extends SnapshotMergeOp
	{
		private int _badFormatLines = 0;
				
		private int _subErrTotal = 0;
			
		
		public ExMergeOp(ExelateUserQueue exq, String daycode, SimpleMail logmail)
		{
			super(exq, daycode, logmail);
		}
	
		private void grabDumpFile() throws IOException
		{
			Configuration s3conf = new Configuration();
			s3conf.setStrings("fs.default.name", "s3n://exelate-incoming/");
			FileSystem fsys = FileSystem.get(s3conf);
			
			BufferedWriter bwrite = FileUtils.getWriter(getRawDumpPath());
			
			for(int dpi = 0; dpi < 20; dpi++)
			{
				Path dumppath = new Path(getS3NDumpPath(_dayCode, dpi));
				
				// Util.massert(fsys.exists(dumppath) || dpi > 0, 
				//	"First dump path %s does not exist", dumppath);
				
				if(!fsys.exists(dumppath) && dpi == 0)
				{
					_logMail.pf("WARNING, no dump file found for  daycode %s\n", _dayCode);
				}
				
				if(!fsys.exists(dumppath))
				{
					_logMail.pf("Dumppath %s does not exists, breaking\n", dumppath);
					break; 
				}
				
				int lcount = 0;
				BufferedReader bread = HadoopUtil.getGzipReader(fsys, dumppath);
				for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
				{
					lcount++;
					bwrite.write(oneline);
					bwrite.write("\n");
				}
				bread.close();
				
				_logMail.pf("Downloaded %d lines for dump path %s\n", lcount, dumppath);
			}
			bwrite.close();
		}		
		
		private void prepDumpFile() throws IOException
		{
			BufferedReader bread = FileUtils.getReader(getRawDumpPath());
			BufferedWriter bwrit = FileUtils.getWriter(getPrcDumpPath());
			
			int readlines = 0;
			int writlines = 0;
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				readlines++;
				
				// Timestamp, country code, wtp, segment string
				String[] ts_cc_wtp_seg = oneline.split("\t");
				if(ts_cc_wtp_seg.length != 4)
				{ 
					_badFormatLines++;
					continue;
				}
				
				// Check that the WTP is valid				
				String wtpstr = ts_cc_wtp_seg[2];				
				WtpId wid = WtpId.getOrNull(wtpstr);
				if(wid == null)
				{
					// Util.pf("Found invalid WTP ID %s\n", wtpstr);
					_badFormatLines++;
					continue; 
				}		
				
				bwrit.write(wid.toString());
				bwrit.write("\t");
				bwrit.write(ts_cc_wtp_seg[3]);
				bwrit.write("\t");
				bwrit.write(ts_cc_wtp_seg[1]);
				bwrit.write("\n");
				writlines++;
			}
			
			bread.close();
			bwrit.close();
			
			_logMail.pf("Finished preprocessing, read %d lines, wrote %d lines\n", readlines, writlines);
		}
		
		private String getRawDumpPath()
		{
			return Util.sprintf("%s/RAW_DUMP_%s.txt", EX_PATH_MAN.getLocalDir(), _dayCode);
		}
		
		private String getPrcDumpPath()
		{
			return SegmentPathMan.Party3Type.exelate.getProcFilePath(_dayCode);
		}		
		
		public void runOp() throws IOException
		{			
			grabDumpFile();
			prepDumpFile();
			
			buildSortMap();
			
			// The Sing-Q must be set before the merge operation starts
			// This is now done elsewhere
			// setLocalSingQ(TimeUtil.dayBefore(_dayCode));			
			
			merge();
						
			finishUp(3);
			
			// This data man has an extra file, the raw dump file, that we need to clean up here
			deleteRaw();
			
			_logMail.send2admin();
		}
		
		private void deleteRaw() throws IOException
		{
			File rawdump = new File(getRawDumpPath());	
			
			if(rawdump.exists())
				{  rawdump.delete(); }			
		}
		
		private void logErrorLines(List<String> errlist)
		{
			for(String oneerr : errlist)
			{
				if(_subErrTotal < 1000)
				{ 
					_logMail.pf("Error on line %s\n", oneerr); 
					_subErrTotal++;
				}
			}			
		}
		
		/*
		void doMerge() throws IOException
		{
			_logMail.pf("Starting merge operation...\n");

			// Okay, preprocessing ensures that leading column is the WTP, so we don't have to modify the sort column info			
			SortedFileMap sortmap = new SortedFileMap(FileUtils.getReader(getPrcDumpPath()));
						
			// PrintWriter pwrite = HadoopUtil.getHdfsWriter(_fSystem, getGimpPath(_dayCode));
			BufferedWriter pwrite = FileUtils.getWriter(EX_PATH_MAN.getGimpPath(_dayCode));
			
			int writecount = 0;
			ExelateUserQueue exQ = getSingQ();

			while(exQ.hasNext())
			{
				ExUserPack exupnext = exQ.nextPack();

				while(!sortmap.isEmpty() && sortmap.firstKey().compareTo(exupnext.wtpid) < 0)
				{
					// if(Math.random() < .001)
					//	{ Util.pf("Found new ID from sort file with ID %s\n", _dataMap.firstKey()); }
					
					// This is a new WTP id that is not in the Master file, and was found for the first time today
					Map.Entry<String, List<String>> ent = sortmap.pollFirstEntry();

					ExUserPack exupnew = new ExUserPack(ent.getKey());
					exupnew.integrateNewData(ent.getValue(), _dayCode);
					logErrorLines(exupnew.errlist);
					
					writecount += exupnew.write(pwrite);
					_newUsers++;
					_newTotal++;
				}
				
				if(!sortmap.isEmpty() && sortmap.firstKey().equals(exupnext.wtpid))
				{
					// Here there is data in both the Master file and the day's slurped data
					Map.Entry<String, List<String>> ent = sortmap.pollFirstEntry();
					exupnext.integrateNewData(ent.getValue(), _dayCode);
					logErrorLines(exupnext.errlist);
					_updateUsers++;
					
				} else {
					_noUpdateUsers++;	
				}
				
				_newTotal++;
				writecount += exupnext.write(pwrite);
				
				// Util.pf("Write count is %d\n", writecount);
				
				if((_newTotal % 1000) == 0)
				{
					// Util.pf("New total is %d\n", _newTotal);	
					
				}
				
				if((exQ.polledUsers % 10000) == 0)
				{
					Util.pf(".");
				}
			}
			
			while(!sortmap.isEmpty())
			{
				// Write out remaining data.
				Map.Entry<String, List<String>> ent = sortmap.pollFirstEntry();
				ExUserPack exupnew = new ExUserPack(ent.getKey());
				
				if(Math.random() < .001)
					{ Util.pf("Found new ID from sort file with ID %s\n", ent.getKey()); }
				
				exupnew.integrateNewData(ent.getValue(), _dayCode);
				logErrorLines(exupnew.errlist);

				writecount += exupnew.write(pwrite);	
				_newTotal++;				
			}
						
			pwrite.close();
			
			_logMail.pf("Finished merge, stats: \n\t%d updated users\n\t%d non-updated\n\t%d prev total\n\t%d new total\n",
			 	_updateUsers, _noUpdateUsers, exQ.polledUsers, _newTotal);
			
			_logMail.pf("Master file size: \n%d lines before\n%d lines after\n", exQ.linesRead, writecount);
		}		
		*/
	}
		
	
	public static class ExelateUserQueue extends SegmentDataQueue
	{
		private boolean _isNullMode = false;
		
		public ExelateUserQueue(LineReader bread) throws IOException
		{
			super(Party3Type.exelate, bread);
		}		

		public void setNullMode()
		{
			_isNullMode = true;	
		}
		
		public ExUserPack lookup(String userid) throws IOException
		{
			if(_isNullMode)
				{ return null; }			
			
			Map.Entry<String, List<String>> myentry = lookupEntry(userid);
			return (myentry == null ? null : ExUserPack.build(myentry));
		}

		public ExUserPack nextPack() throws IOException
		{
			Util.massert(!_isNullMode, "Attempt to call nextPack(..) in null mode");
			return ExUserPack.build(nextEntry());
		}			
		
		public ExUserPack buildEmpty(String wtpid)
		{
			return new ExUserPack(wtpid);	
		}
	}
	
	public static class ExUserPack extends SegmentPack.IntPack
	{
		List<String> errlist = Util.vector();
		
		ExUserPack(String wtpid)
		{
			super(wtpid);	
		}
		
		ExUserPack(Map.Entry<String, List<String>> myentry)
		{
			super(myentry);
		}
			
		private static ExUserPack build(Map.Entry<String, List<String>> myentry)
		{
			return new ExUserPack(myentry);
		}
		
		@Override
		void integrateNewData(Collection<String> newlines, String daycode)
		{
			// Util.pf("Calling integrate for user %s, %d newlines\n", wtpid, newlines.size());
			
			for(String oneline : newlines)
			{
				try {		
					
					// WTP, SEG, CTRY
					String[] wtp_seg_ctry = oneline.split("\t");
					
					Util.massert(wtp_seg_ctry.length == 3, "Badly formatted row got past preprocessor");
										
					// Check that the WTP is valid
					WtpId wid = WtpId.getOrNull(wtp_seg_ctry[0]);
					
					Util.massert(wid != null, "Badly formatted WTP ID %s got past preprocessor", wtp_seg_ctry[0]);
					Util.massert(wtpid.equals(wtp_seg_ctry[0]), 
						"Associated with user %s found line %s", wtpid, oneline);
					
					String[] segs = wtp_seg_ctry[1].split(",");
					
					for(String oneseg : segs)
					{
						int dashindex = oneseg.indexOf("-", 1);
						if(dashindex == -1)
							{ throw new NumberFormatException(oneseg); }
						
						String daybef = oneseg.substring(0, dashindex);
						Integer segid = Integer.valueOf(oneseg.substring(dashindex+1));
											
						if(daybef.startsWith("!!"))
						{
							_segData.remove(segid);	
							continue;
						}
						
						// This is a "typical" error, don't need to blow away the entire line.
						int daybefint = Integer.valueOf(daybef);
						if(daybefint < -100 || daybefint > 10000)
							{ continue; }
						
						String prevdaycode = ExelateDataMan.findDayBefore(daybefint, daycode);
						_segData.put(segid, prevdaycode);
					}
					
					if(!wtp_seg_ctry[2].startsWith("!!"))
					{
						//Util.pf("Found specdata = %s", wtp_seg_ctry[2]);
						_specData.put(SpecialData.country, wtp_seg_ctry[2]);
					}
					
				} catch (Exception ex) {
					
					Util.pf("Exception due to badly formatted row? : \n%s\n", oneline);
					ex.printStackTrace();
					errlist.add(oneline);
				}						
			}	
		}
	}	
}
