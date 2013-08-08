
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapred.*; 
//import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;
import com.adnetik.shared.Util.ExcName;
import com.adnetik.shared.BidLogEntry.*;

public class ResourceInfoTest extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		int exitCode = HadoopUtil.runEnclosingClass(args);
	}
	
	public int run(String[] args) throws Exception
	{		
		// Make sure we are using both .txt and .lzo
		getConf().setBoolean("lzo.text.input.format.ignore.nonlzo", false);				
		
		JobConf job = new JobConf(getConf(), this.getClass());
		FileSystem fSystem = FileSystem.get(getConf());		
		String partcode = "0";
		partcode = Util.padLeadingZeros(Integer.valueOf(partcode), 5); 
		String canonday = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
		
		{
			// Util.massert(!(new File("local_names.txt")).exists(), "Local names already exists");
			
			Map<String, String> resmap = getResourceMap();
			List<String> outlist = Util.vector();
			
			for(String reskey : resmap.keySet())
			{
				String oneline = Util.sprintf("%s\t%s", reskey, resmap.get(reskey));
				outlist.add(oneline);
			}

			FileUtils.writeFileLinesE(outlist, "local_data.txt");
			// System.exit(1);
			
		}				
		// System.exit(1);
		
					
		{
			List<String> daylist = UserIndexUtil.getCanonicalDayList();
			
			List<Path> pathlist = Util.vector();
			for(String daycode : daylist)
			{
				String spath = Util.sprintf("/userindex/sortscrub/%s/part-%s.*", daycode, partcode);
				pathlist.add(new Path(spath));
				break; // just need one
			}
						
			Util.pf("\nFound %d paths", pathlist.size());
			
			FileInputFormat.setInputPaths(job, pathlist.toArray(new Path[] {}));
			job.setInputFormat(com.hadoop.mapred.DeprecatedLzoTextInputFormat.class);
		}
		
					
		{
			Text a = new Text("");
			// HadoopUtil.alignJobConf(job, new EncodingTestMapper(), new ScoreUserJob.ScoreReducer(), a, a, a, a);
			HadoopUtil.alignJobConf(job, new ResourceInfoMapper(), new HadoopUtil.SetReducer(), a, a, a, a);
			
		}
		
		{
			String outputPath = Util.sprintf("/userindex/testresource", canonday, partcode);
			HadoopUtil.moveToTrash(this, new Path(outputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		}
			
		// Specify various job-specific parameters 
		job.setJobName(Util.sprintf("Resource Load test"));
		
		job.setPartitionerClass(ScoreUserJob.Reshuffler.class);
		
		// Submit the job, then poll for progress until the job is complete
		JobClient.runJob(job);	

		{
			Path partfileout = new Path("/userindex/testresource/part-00000");
			List<String> hadlines = HadoopUtil.readFileLinesE(fSystem, partfileout);
			FileUtils.writeFileLinesE(hadlines, "hadoop_data.txt");
			//List<String> hadlines = HadoopUtil.readFileLinesE(
			
		}
		
		
		return 1;

	}

	public static Map<String, String> getResourceMap() throws IOException
	{
		Map<String, String> rmap = Util.treemap();		

		List<String> featlist = new Vector<String>(StrayerFeat.getFeatMap().keySet());
		
		for(int i = 0; i < featlist.size(); i++)
		{
			String fid = Util.sprintf("fid____%d", i);
			rmap.put(fid, "" + featlist.get(i).hashCode());	
		}
		
		/*
		GoogleVertLookup gvl = GoogleVertLookup.getSing();
		
		for(Integer pcode : gvl.baseCodeMap.keySet())
		{
			String fid = Util.sprintf("fid____%d", pcode);
			rmap.put(fid, gvl.baseCodeMap.get(pcode));
		}
		*/
		
		// rlist.addAll(gvl.prntCodeMap.values());
		
		/*
		for(AdaBoost.BinaryFeature<UserPack> bfeat : StrayerFeat.getGoogVerts())
		{
			String fid = Util.sprintf("fid____%d", rmap.size());
			rmap.put(fid, bfeat.toString());
		}
		*/
		
		return rmap;
		
		/*
		InputStream resource = ResourceInfoTest.class.getResourceAsStream(GoogleVertLookup.VERT_CSV_PATH);
		InputStreamReader ireader = new InputStreamReader(resource, "UTF-8");
		
		List<String> rlist = Util.vector();
		Scanner sc = new Scanner(ireader);
		
		while(sc.hasNextLine())
		{
			String s = sc.nextLine().trim();
			rlist.add(s);
		}
		
		sc.close();		
		return rlist;
		*/
	}
	
	// TODO: put all of these mappers and reducers in their own file
	public static class ResourceInfoMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		Integer myCount = 0;
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
		throws IOException
		{		
			if(myCount < 10)
			{				
				Map<String, String> resmap = getResourceMap();
				
				for(String rkey : resmap.keySet())
				{
					output.collect(new Text(rkey), new Text(resmap.get(rkey)));
				}
				
				myCount++;
			}
		}
	}		
}

