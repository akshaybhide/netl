
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

// import com.adnetik.userindex.UserIndexUtil;

// This class mirrors structure of 
public class DigiPixelDataMan
{
	
	public static SegmentPathMan DP_PATH_MAN = new SegmentPathMan(SegmentPathMan.Party3Type.digipixel);
	
	private static DigiQueue _SING_QUEUE;
	
	public static void main(String[] args) throws IOException
	{
		if(args.length < 1)
		{
			Util.pf("Usage: DigiPixelDataMan <daycode>\n");
			return;
		}
		
		String daycode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode %s", daycode);

		
		DigiMergeDaemon dmd = new DigiMergeDaemon(TimeUtil.dayAfter(daycode));
		dmd.startDaemon();
	}
	
	private static String dumpFilePath(String daycode)
	{
		return Util.sprintf("%s/dumpfile/dump_%s.txt", DP_PATH_MAN.getLocalDir(), daycode);
	}
	
	// This is where UserIndex StagingInfoManager puts the strip files.
	private static String stripDirPath(String daycode)
	{
		return Util.sprintf("%s/strip/%s", UserIndexUtil.LOCAL_UINDEX_DIR, daycode);	
	}
	
	public static DigiQueue getSingQ()
	{
		Util.massert(_SING_QUEUE != null, "Must initialize with setSingQ(..) first");
		return _SING_QUEUE;
	}
	
	private static DigiQueue getLocalSingQ(String daycode) throws IOException
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode: %s", daycode);
		
		String localmaster = DP_PATH_MAN.getLocalMasterPath(daycode);
		Util.massert((new File(localmaster)).exists(), "Could not find local snapshot file %s", localmaster);
		
		BufferedReader bread = FileUtils.getReader(localmaster);
		return new DigiQueue(FileUtils.bufRead2Line(bread));	
	}
	
	public static void setSingQ(String daycode) throws IOException
	{
		Util.massert(TimeUtil.checkDayCode(daycode), "Invalid daycode: %s", daycode);
		
		String snappath = DP_PATH_MAN.getHdfsMasterPath(daycode);
		FileSystem fsys = FileSystem.get(new Configuration());
		Util.massert(fsys.exists(new Path(snappath)), "Could not find snapshot file %s", snappath);
		
		BufferedReader bread = HadoopUtil.hdfsBufReader(fsys, snappath);
		DigiQueue dq = new DigiQueue(FileUtils.bufRead2Line(bread));	
		_SING_QUEUE = dq;
	}
	
	public static class DigiMergeDaemon extends DailyDaemon
	{
		public DigiMergeDaemon(String dc)
		{
			super(dc);	
		}
		
		protected  String getShortStartTimeStamp()
		{ 
			// No special reason here, just feels smart to space it out a bit
			return "14:00:00"; 
		}
		
		public void runProcess()
		{
			try {
				DigiMergeOp dmo = new DigiMergeOp(getPrevDayCode());
				dmo.runOp();				
				
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);	
			}
		}
	}	
	
	
	private static class DigiMergeOp
	{
		private String _dayCode;

		private int _slurpLines = 0;
		private int _newUsers = 0;
		private int _newTotal = 0;				
		private int _updateUsers = 0;
		private int _noUpdateUsers = 0;
		
		private int _badFormatLines = 0;
				
		private TreeMap<String, List<String>> _dataMap = Util.treemap();

		private TreeMap<Integer, String> _dayBeforeMap = Util.treemap();
		
		private SimpleMail _logMail;
		
		private int _subErrTotal = 0;
		
		private BufferedReader _sortDumpReader;
		private boolean _doneReading = false;
		
		public DigiMergeOp(String dc) throws IOException
		{
			_dayCode = dc;	

			String prevmaster = DP_PATH_MAN.getLocalMasterPath(TimeUtil.dayBefore(_dayCode));
			Util.massert(FileUtils.pathExists(prevmaster), 
				"Local master list %s not found, aborting", prevmaster);
			
			_logMail = new SimpleMail(Util.sprintf("DigiPixelMergeOp for %s -> %s", TimeUtil.dayBefore(dc), dc));
		}
		
		private Set<String> getPixelNameSet()
		{
			Set<String> pathset = Util.treeset();
			File stripdir = new File(stripDirPath(_dayCode));
			
			for(File onefile : stripdir.listFiles())
			{
				if(onefile.getName().endsWith("strip"))
					{ pathset.add(onefile.getName()); }
			}
			return pathset;
		}
		
		
		private void compilePixelDump() throws IOException
		{
			Set<String> pixnameset = getPixelNameSet();
			_logMail.pf("Found %d pixel strip files\n", pixnameset.size());
			
			File stripdir = new File(stripDirPath(_dayCode));
			BufferedWriter dumpwrite = FileUtils.getWriter(dumpFilePath(_dayCode));
						
			for(int pixid = 0; !pixnameset.isEmpty(); pixid++)
			{
				String pixname = Util.sprintf("pixel_%d.strip", pixid);
				if(pixnameset.contains(pixname))
				{
					// Util.pf("Pixid is %d, Found file %s\n", pixid, pixname);
					Util.pf(".");
					String fullpath = Util.sprintf("%s/%s", stripdir, pixname);

					List<String> idlist = FileUtils.readFileLinesE(fullpath);
					
					for(String oneid : idlist)
					{
						String towrite = Util.sprintf("%s\t%d\n", oneid, pixid);
						dumpwrite.write(towrite);
						_slurpLines++;
					}					
					
					pixnameset.remove(pixname);
				}
			}
			
			dumpwrite.close();
		}
		
		private void sortPixelDump() throws IOException
		{
			
			// _logMail.pf("Going to sort the pixel dump file...");
			Util.unixsort(dumpFilePath(_dayCode), "");
			// _logMail.pf(" ... done with sort");
		}		
		
		void doMerge() throws IOException
		{
			FileSystem fsys = FileSystem.get(new Configuration());
			_logMail.pf("Starting merge operation...\n");
			
			SortedFileMap sfmap = SortedFileMap.buildFromFile(new File(dumpFilePath(_dayCode)), false);
			
			BufferedWriter pwrite = FileUtils.getWriter(DP_PATH_MAN.getGimpPath(_dayCode));
			
			int writecount = 0;
			DigiQueue digiQ = getLocalSingQ(TimeUtil.dayBefore(_dayCode));
			
			while(digiQ.hasNext())
			{
				DigiPack diginext = digiQ.nextPack();

				while(!sfmap.isEmpty() && sfmap.firstKey().compareTo(diginext.wtpid) < 0)
				{
					// if(Math.random() < .001)
					//	{ Util.pf("Found new ID from sort file with ID %s\n", _dataMap.firstKey()); }
					
					// This is a new WTP id that is not in the Master file, and was found for the first time today
					Map.Entry<String, List<String>> ent = sfmap.pollFirstEntry();

					DigiPack diginew = new DigiPack(ent.getKey());
					diginew.integrateNewLines(ent.getValue(), _dayCode);
					// logErrorLines(exupnew.errlist);
					
					writecount += diginew.write(pwrite);
					_newUsers++;
					_newTotal++;
				}
				
				if(!sfmap.isEmpty() && sfmap.firstKey().equals(diginext.wtpid))
				{
					// Here there is data in both the Master file and the day's slurped data
					Map.Entry<String, List<String>> ent = sfmap.pollFirstEntry();
					diginext.integrateNewLines(ent.getValue(), _dayCode);
					// logErrorLines(exupnext.errlist);
					_updateUsers++;
					
				} else {
					_noUpdateUsers++;	
				}
				
				_newTotal++;
				writecount += diginext.write(pwrite);
				
				// Util.pf("Write count is %d\n", writecount);
							
				if((digiQ.polledUsers % 10000) == 0)
				{
					Util.pf(".");
				}
			}
			
			while(!sfmap.isEmpty())
			{
				// Write out remaining data.
				Map.Entry<String, List<String>> ent = sfmap.pollFirstEntry();
				DigiPack diginew = new DigiPack(ent.getKey());
				
				if(Math.random() < .001)
					{ Util.pf("Found new ID from sort file with ID %s\n", ent.getKey()); }
				
				diginew.integrateNewLines(ent.getValue(), _dayCode);
				// logErrorLines(exupnew.errlist);

				writecount += diginew.write(pwrite);	
				_newTotal++;				
			}
						
			pwrite.close();
			
			_logMail.pf("Finished merge, stats: \n\t%d updated users\n\t%d non-updated\n\t%d prev total\n\t%d new total\n",
			 	_updateUsers, _noUpdateUsers, digiQ.polledUsers, _newTotal);
			
			_logMail.pf("Master file size: \n%d lines before\n%d lines after\n", digiQ.linesRead, writecount);
			
		}			
		
		public void runOp() throws IOException
		{
			// setSingQ(TimeUtil.dayBefore(_dayCode));
			
			double startup = Util.curtime();
			compilePixelDump();
			sortPixelDump();
			double midpoint = Util.curtime();
			doMerge();
			double fintime = Util.curtime();
			
			_logMail.pf("Finished DigiPixel Merge, slurp took %.03f, merge took %.03f, total %.03f\n",
				(midpoint-startup)/1000, (fintime-midpoint)/1000, (fintime-startup)/1000);
			
			_logMail.pf("Dump files lines %d, badly formatted lines %s, sub error lines %d\n",
				_slurpLines, _badFormatLines, _subErrTotal);
			
			finishUp();
			
			_logMail.send2admin();
		}
	
		public void finishUp() throws IOException
		{
			DP_PATH_MAN.renameGimp2Master(_dayCode);
			DP_PATH_MAN.uploadMaster(_dayCode, _logMail);
			DP_PATH_MAN.deleteOldLocalMaster(_dayCode, 3, _logMail);
			
			// Delete the temp file
			(new File(dumpFilePath(_dayCode))).delete();
		}
	}
			
	public static class DigiQueue extends SegmentDataQueue
	{
		public DigiQueue(LineReader bread) throws IOException
		{
			super(bread);
		}		

		public DigiPack lookup(String userid) throws IOException
		{
			Map.Entry<String, List<String>> myentry = lookupEntry(userid);
			return (myentry == null ? null : DigiPack.build(myentry));
		}

		public DigiPack nextPack() throws IOException
		{
			return DigiPack.build(nextEntry());
		}			
	}
	
	public static class DigiPack extends SegmentDataQueue.SegmentPack
	{
		List<String> errlist = Util.vector();
		
		DigiPack(String wtpid)
		{
			super(wtpid);	
		}
		
		DigiPack(Map.Entry<String, List<String>> myentry)
		{
			super(myentry);
		}
			
		private static DigiPack build(Map.Entry<String, List<String>> myentry)
		{
			return new DigiPack(myentry);
		}
		
		private void  integrateNewLines(Collection<String> newlines, String daycode)
		{
			for(String oneline : newlines)
			{
				String[] wtp_pixid = oneline.split("\t");
								
				Util.massert(wtpid.equals(wtp_pixid[0]), 
					"Associated with user %s found line %s", wtpid, oneline);
				
				int pixid = Integer.valueOf(wtp_pixid[1]);
				
				_segData.put(pixid, daycode);
			}
		}
	}	
}
