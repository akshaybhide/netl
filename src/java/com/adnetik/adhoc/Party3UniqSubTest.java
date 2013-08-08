
package com.adnetik.adhoc;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

import com.adnetik.data_management.*;
import com.adnetik.data_management.ExelateDataMan.*;
import com.adnetik.data_management.BluekaiDataMan.*;
import com.adnetik.analytics.ThirdPartyDataUploader.*;


public class Party3UniqSubTest extends Configured implements Tool
{
	SimpleMail _logMail;
	String _dayCode;
	
	public enum ScanData { FoundExUsers, FoundBkUsers, TotalUsers };
	
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{		
		if(args.length < 1)
		{
			Util.pf("Party3UniqueScan <daycode>\n");
			return 1;
		}
		
		_dayCode = args[0];
		_dayCode = "yest".equals(_dayCode) ? TimeUtil.getYesterdayCode() : _dayCode;
		_dayCode = "yestyest".equals(_dayCode) ? TimeUtil.dayBefore(TimeUtil.getYesterdayCode()) : _dayCode;
		Util.massert(TimeUtil.checkDayCode(_dayCode), "Invalid day code %s", _dayCode);
		
		_logMail = new SimpleMail("Party3UniqueScan for " + _dayCode);
		int excode = runsub(args);
		
		// _logMail.send2admin();
		return excode;
	}
	
	private int runsub(String[] args) throws IOException
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		Map<String, String> optargs = Util.getClArgMap(args);
		Set<Part3Code> partset = Util.treeset();
		for(Part3Code p3c : Part3Code.values())
		{
			partset.add(p3c);

			if("false".equals(optargs.get("use_" + p3c.toString().toLowerCase())))
			{
				_logMail.pf("DECLINING to use %s data\n", p3c);
				partset.remove(p3c); 
			}
		}
		
		FileSystem fsys = FileSystem.get(new Configuration());
		
		// Check to make sure the previous day's master list data is available
		if(!checkMasterListData(fsys, _dayCode, partset))
		{
			_logMail.pf("WARNING!!!!! Problem with master list data, aborting\n");
			return 1;
		}
		
		{
			List<Path> pathlist = Util.vector();
			for(LogType onetype : new LogType[] { LogType.imp, LogType.click, LogType.conversion })
			{
				String lzopatt = Util.sprintf("/data/%s/*%s*.lzo", onetype, _dayCode);
				pathlist.addAll(HadoopUtil.getGlobPathList(fsys, lzopatt));
				// pathlist.add(new Path("/data/imp/yahoo_2012-10-01_v19.lzo"));
				_logMail.pf("Found %d input paths for logtype=%s\n", pathlist.size(), onetype);				
			}

			if(pathlist.isEmpty())
			{ 
				_logMail.pf("No valid LZO paths found, aborting\n");
				return 1;
			}
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {} ));		
		}
		
		
		job.setStrings("DAY_CODE", _dayCode);	
		job.setStrings("PART3CODES", Util.join(partset, ","));
		
		
		// Align Job
		{
			Text a = new Text("");	
			LongWritable b = new LongWritable(1);
 			HadoopUtil.alignJobConf(job, new CampUuidMapper(), new HadoopUtil.CountReducer(), a, a, a, b);	
		}
		
		// Deal with output path
		{
			Path outputpath = new Path(Util.sprintf("/thirdparty/uniqs/subtest/%s", _dayCode));
			Util.pf("\nTarget Output path is %s", outputpath);
			HadoopUtil.moveToTrash(this, outputpath);
			// FileSystem.get(getConf()).delete(outputPath, true);
			//HadoopUtil.checkRemovePath(FileSystem.get(getConf()), outputPath);
			FileOutputFormat.setOutputPath(job, outputpath);	
		}	
		
		job.setJobName("Party3UniqSubTest " + _dayCode);
		
		// Submit the job, then poll for progress until the job is complete
		RunningJob rjob = JobClient.runJob(job);	
		
		// Record the counters in the job
		HadoopTools.counterMap2Mail(rjob, _logMail);

		return 0;		
	}
	
	
	private boolean checkMasterListData(FileSystem fsys, String daycode, Set<Part3Code> partset) throws IOException
	{
		List<Path> pathlist = Util.vector();
		
		if(partset.contains(Part3Code.BK)) 
			{ Path bkmaster = new Path(BluekaiDataMan.BK_PATH_MAN.getHdfsMasterPath(daycode)); }
		
		if(partset.contains(Part3Code.EX)) 
			{ Path exmaster = new Path(ExelateDataMan.EX_PATH_MAN.getHdfsMasterPath(daycode)); }
		
		for(Path onepath : pathlist)
		{
			if(!fsys.exists(onepath))
			{
				_logMail.pf("Master list path %s does not exist, aborting\n", onepath);	
				return false;
			}			
		}
		
		return true;
	}
	
	public static class CampUuidMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		PathInfo _pInfo = null;
		
		
		@Override
		public void map( LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter rep)
		throws IOException
		{
			if(_pInfo == null)
			{
				String path = rep.getInputSplit().toString();
				_pInfo = PathInfo.fromLzoPath(path);				
			}
			
			BidLogEntry ble = BidLogEntry.getOrNull(_pInfo.pType, _pInfo.pVers, value.toString());
			if(ble == null)
				{ return; }
			
			WtpId wid = WtpId.getOrNull(ble.getField(LogField.wtp_user_id).trim());
			if(wid == null)
				{ return; }
						
			// Increase interactivity
			//if(wtpid.compareTo("1") > 0)
			//	{ return; }
			
			Integer campid = ble.getIntField(LogField.campaign_id);
			Integer lineid = ble.getIntField(LogField.line_item_id);
			String linetype = ble.getField(LogField.line_item_type);
			linetype = linetype.trim().length() == 0 ? "NOTSET" : linetype;
			
			String combkey = Util.join(new Object[] { wid.toString(), campid, lineid, linetype, _pInfo.pType }, Util.DUMB_SEP);
			
			output.collect(new Text(combkey), HadoopUtil.TEXT_ONE);
			
		} 
	}	
}

