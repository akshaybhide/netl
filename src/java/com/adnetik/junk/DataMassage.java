
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

// Handles the work of generating a feature report and learning a classifier.
public class DataMassage
{	
	public static void main(String[] args) throws IOException
	{
		List<String> datelist = TimeUtil.getDateRange(100);
		Configuration conf = new Configuration();
		FileSystem fsys = FileSystem.get(conf);
		
		for(String daycode : datelist)
		{
			LocalMode.IdHashPack idpack = new LocalMode.IdHashPack(daycode);
			
			List<Path> negpathlist;
			{
				String negpatt = Util.sprintf("/userindex/neg_cands/cands_%s.txt", daycode);
				negpathlist = HadoopUtil.getGlobPathList(fsys, negpatt);
			}

			if(negpathlist.isEmpty())
				{ continue; }
			
			Util.pf("Found %d negative paths for day code %s\n", negpathlist.size(), daycode);
			
			for(Path oneneg : negpathlist)
			{
				List<String> neglines = HadoopUtil.readFileLinesE(oneneg);
								
				for(String oneline : neglines)
				{
					String[] toks = oneline.split("\t");
					if(toks.length < 2)
						{ continue; }
					
					String ccode = toks[1];
					String wtpid = toks[0];
					idpack.reportUniqueCandidate(wtpid, ccode);
					// Util.pf("Line is %s\n", oneline);
				}
			}
			
			for(String ccode : idpack.uniqPackMap.keySet())
			{
				Util.pf("\tFound %d ids for ccode=%s\n", idpack.uniqPackMap.get(ccode).size(), ccode);	
			}
				
			idpack.writeData();
		}
	}		
	
	public static void main3(String[] args) throws IOException
	{
		List<String> datelist = TimeUtil.getDateRange(100);
		Configuration conf = new Configuration();
		FileSystem fsys = FileSystem.get(conf);
		
		for(String daycode : datelist)
		{
			LocalMode.IdHashPack idpack = new LocalMode.IdHashPack(daycode);
			
			List<Path> negpathlist;
			{
				String negpatt = Util.sprintf("/userindex/negpools/pool_%s/part-*", daycode);
				negpathlist = HadoopUtil.getGlobPathList(fsys, negpatt);
			}

			if(negpathlist.isEmpty())
				{ continue; }
			
			Util.pf("Found %d negative paths for day code %s\n", negpathlist.size(), daycode);
			
			for(Path oneneg : negpathlist)
			{
				List<String> neglines = HadoopUtil.readFileLinesE(oneneg);
				
				if(neglines.isEmpty())
					{ continue; }
				
				for(String oneline : neglines)
				{
					String[] toks = oneline.split("\t");
					String ccode = toks[0];
					String wtpid = toks[1];
					idpack.dumbAddCandidate(wtpid, ccode);
				}
				
			}
			
			for(String ccode : idpack.uniqPackMap.keySet())
			{
				Util.pf("\tFound %d ids for ccode=%s\n", idpack.uniqPackMap.get(ccode).size(), ccode);	
			}
				
			idpack.writeData();
		}
	}	
	
	
	public static void main2(String[] args) throws IOException
	{
		String daycode = args[0];
		
		LocalMode.IdHashPack idpack = new LocalMode.IdHashPack(daycode);
		
		Util.pf("Hello from DataMassage, daycode is %s\n", daycode);
		
		int lcount = 0;
		Scanner sc = new Scanner(System.in);
		
		while(sc.hasNextLine())
		{
			String logline = sc.nextLine();
			String[] toks = logline.split("\t");
			
			String wtpid = toks[0];
			String ccode = toks[1];

			idpack.reportUniqueCandidate(wtpid, ccode);

			lcount++;
			
			if((lcount % 10000) == 0)
			{
				Util.pf("Finished processing line %d\n", lcount);	
			}
		}
		
		idpack.writeData();
		
		sc.close();
	}
}
