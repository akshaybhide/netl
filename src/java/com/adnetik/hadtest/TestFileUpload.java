
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

public class TestFileUpload extends Configured implements Tool
{
	public static final int MAX_VAL = 1000;	
	public static final String TEST_FILE_PATH = "/mnt/burfoot/hadtest/fakedata.txt";
	
	public static void main(String[] args) throws Exception
	{
		//System.out.printf("\nFile %s--", "file:///");
		int exitCode = ToolRunner.run(new TestFileUpload(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception
	{
		// Create a new JobConf
		JobConf job = new JobConf(getConf(), this.getClass());

		FileSystem fSystem = FileSystem.get(getConf());				

		OutputStream fos = fSystem.create(new Path(TEST_FILE_PATH));
		
		for(int i = 0; i < MAX_VAL; i++)
		{
			for(int j = 0; j < MAX_VAL; j++)
			{
				String line = Util.sprintf("\n%d\t%d", i, j);
				fos.write(line.getBytes());
			}
		}
		
		fos.close();
		
		Util.pf("\nFile upload complete\n\n");
		
		return 1;
	}
}
