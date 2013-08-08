
package com.adnetik.data_management;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;

public class BluekaiHadoopMerge  extends Configured implements Tool
{	
	public static void main(String[] args) throws Exception
	{ 
		HadoopUtil.runEnclosingClass(args);
		
		// Map<String, Integer> prefmap = getPrefPartMap();
		// Util.pf("Pref map is %s\n", prefmap);
	}
	  
	public int run(String[] args) throws Exception
	{
		String daycode = args[0];
		daycode = ("yest".equals(daycode) ? TimeUtil.getYesterdayCode() : daycode);
		if(!TimeUtil.checkDayCode(daycode))
		{ 
			Util.pf("\nBad day code : %s" , daycode);
			System.exit(1);
		}
		
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());
		
		// Align Job
		{
			Text a = new Text("");	
			HadoopUtil.alignJobConf(job, false, new HadoopUtil.EmptyMapper(), new HadoopUtil.EmptyReducer(), new OrderWtpPartitioner(), a, a, a, a);		
		}
		
		// With this and the mixed-input flag set above, we can deal with anything. Maybe
		// job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		
		// Deal with input files
		Path masterlist = new Path("/thirdparty/bluekai/MASTER_LIST_2012-10-16.txt");
		FileInputFormat.setInputPaths(job, new Path[]  { masterlist });
		
		// Deal with output path
		{
			Path outputPath = new Path("/thirdparty/bluekai/snapshot/testme");
			Util.pf("\nTarget Output path is %s", outputPath);
			HadoopUtil.moveToTrash(this, outputPath);
			// FileSystem.get(getConf()).delete(outputPath, true);
			//HadoopUtil.checkRemovePath(FileSystem.get(getConf()), outputPath);
			FileOutputFormat.setOutputPath(job, outputPath);	
		}
		
		job.setJobName("BluekaiHadoop Merge " + daycode);
		job.setNumReduceTasks(getPrefPartMap().size()); 	
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	
		
		return 1;		
	}
	
	public static TreeMap<String, Integer> getPrefPartMap()
	{	
		TreeMap<String, Integer> prefmap = Util.treemap();
		for(int i = 0; i < 256; i++)
		{
			String h = Integer.toHexString(i);
			h = Util.padLeadingZeros(h, 2);
			prefmap.put(h, prefmap.size());
		}
		return prefmap;
	}
	
	public static class OrderWtpPartitioner implements Partitioner<Text, Text>
	{
		TreeMap<String, Integer> _prefMap;
		
		public void configure(JobConf jobconf) {}
		
		public int getPartition(Text key, Text value, int numPart)
		{
			if(_prefMap == null)
			{ 
				_prefMap = getPrefPartMap();
			}
			
			Util.massert(_prefMap.lastEntry().getValue()+1 == numPart, 
				"Must choose number of partitions to match size of prefmap, which is %d", _prefMap.size());
			
			String wtppref = key.toString().substring(0,2);
			
			Util.massert(_prefMap.containsKey(wtppref),
				"Could not find prefix %s in prefmap, WTP is %s", wtppref, key);
			
			return _prefMap.get(wtppref);
		}
	}	
}
