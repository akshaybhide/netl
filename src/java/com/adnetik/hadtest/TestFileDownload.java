
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

public class TestFileDownload extends Configured implements Tool
{
	public static void main(String[] args) throws Exception
	{
		//System.out.printf("\nFile %s--", "file:///");
		int exitCode = ToolRunner.run(new TestFileDownload(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());				

		List<String> flines = HadoopUtil.readFileLinesE(fSystem, TestFileUpload.TEST_FILE_PATH);
		
		for(int i = 0; i < flines.size(); i++)
		{
			Util.pf("\n\tLine %d: %s", i, flines.get(i));
		}
		
		Util.pf("\nFile download test complete\n\n");
		
		return 1;
	}
}
