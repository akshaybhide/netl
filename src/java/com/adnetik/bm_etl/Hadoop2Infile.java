package com.adnetik.bm_etl;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.bm_etl.BmUtil.*; // Put all the enums in the same place

import com.adnetik.shared.*;

/**
 * Transform output of Hadoop job into a format compatible with SQL Infile operation
 */
public class Hadoop2Infile extends Configured implements Tool 
{
	SimpleMail logmail = null;
	
	int lineCount = 0;
	int batchNum = 0;
	int addedRows = 0;
	
	String dayCode;
	boolean isInternal = false;
	
	public static final int LINES_PER_BATCH = 100000;
	
	Writer tempWriter;
	File tempFile;
	
	private LinkedList<Double> timerList = Util.linkedlist();
	
	SortedSet<DimCode> dimSet;
	SortedSet<DblFact> dblSet;
	SortedSet<IntFact> intSet;
	
	// campaign ID 
	Set<Integer> campIdSet = Util.treeset();

	public Hadoop2Infile() throws IOException
	{
		logmail = new SimpleMail("Hadoop2Infile report");
		timerList.add((double) System.currentTimeMillis());
		openTempFile();
	}
	
	public static void main(String[] args) throws Exception
	{
		HadoopUtil.runEnclosingClass(args);	
	}
	
	public int run(String[] args) throws IOException
	{		
		if(args.length < 1 || (!"yest".equals(args[0]) && !TimeUtil.checkDayCode(args[0])))
		{
			Util.pf("\nUsage: Hadoop2Infile <daycode>\n");	
			return 1;			
		}
		
		Map<String, String> optargs = Util.getClArgMap(args);
		boolean deleteold = !("false".equals(optargs.get("deleteold")));
		dayCode = ("yest".equals(args[0]) ? TimeUtil.getYesterdayCode() : args[0]);
		
		isInternal = (args.length > 1 && "intern".equals(args[1]));
		
		dimSet = DatabaseBridge.getDimSet(AggType.ad_general, DbTarget.external);
		dblSet = DatabaseBridge.getDblFactSet(AggType.ad_general, DbTarget.external);
		intSet = DatabaseBridge.getIntFactSet(AggType.ad_general, DbTarget.external);
		
		// Delete old data
		if(deleteold) 
		{
			logmail.pf("Deleting old data for %s... ", dayCode);
			int delrows = DatabaseBridge.deleteOld(AggType.ad_general, dayCode, DbTarget.external);
			logmail.pf(" ... done, deleted %d rows ", delrows);
		}		
				
		transformResult(FileSystem.get(getConf()));
				
		// Pull from v__ad_general_all to ad_general_all, isgeneral=true
		View2Hard.diffUpdate(campIdSet, dayCode, true, logmail);
		
		{
			List<String> recplist = Util.vector();
			recplist.add("raj.joshi@digilant.com");
			recplist.add("krishna.boppana@digilant.com");
			recplist.add("daniel.burfoot@digilant.com");
			logmail.send(recplist);
		}
		
		return 1;
	}
	
	public void transformResult(FileSystem fsys) throws IOException
	{
		// String respatt = Util.sprintf("%soutput/%s/%s/part-*", BmUtil.BASE_HDFS_DIR, dayCode, (isInternal ? "intern" : "extern"));
		String respatt = BmUtil.getOutputPath(dayCode, DbTarget.external) + "part-*";
		List<Path> pathlist = HadoopUtil.getGlobPathList(fsys, respatt);	
		
		//FileWriter target = new FileWriter(localfile);
		
		messagePf("\nFound %d paths for pattern %s", pathlist.size(), respatt);
		
		for(Path p : pathlist)
		{
			BufferedReader bufread = HadoopUtil.hdfsBufReader(fsys, p);
			line2line(bufread);
			//Util.pf("\nFound path %s", p.toString());
			bufread.close();
		}
		
		// Final upload
		doUpload();
	}
	
	void messagePf(String mssg, Object... vargs)
	{
		String s = Util.sprintf(mssg, vargs);
		Util.pf("%s", s);
		
		if(logmail != null)
			{ logmail.addLogLine(s.trim()); }
	}
	
	void outputLine(String oneline) throws IOException
	{
		tempWriter.write(oneline + "\n");
		lineCount++;
		
		if((lineCount % LINES_PER_BATCH) == 0)
		{
			doUpload();
			openTempFile();
		}
	}
	
	// Never have separate tempFile cleanup; always upload and close/delete at the same time.
	void doUpload() throws IOException
	{
		tempWriter.close();
		addedRows += DatabaseBridge.loadFromFile(tempFile, dimSet, intSet, dblSet, DbTarget.external);
		tempFile.delete();
		batchNum++;
		markFinished();
	}
	
	void openTempFile() throws IOException
	{
		String temppath = Util.sprintf("%s/hadoop2db_tmp_batch_%d.txt", BmUtil.LOCAL_UTIL_DIR, batchNum);
		tempFile = new File(temppath);
		tempWriter = new BufferedWriter(new FileWriter(tempFile));
	}
	
	void markFinished()
	{
		double now = System.currentTimeMillis();
		double secs = (now - timerList.getLast())/1000;
		messagePf("Finished batch %d, lcount=%d, addedrows=%d, took %.02f secs\n", batchNum, lineCount, addedRows, secs);
		timerList.add(now);
	}
	
	
	void line2line(BufferedReader src) throws IOException
	{
		for(String line = src.readLine(); line != null; line = src.readLine())
		{
			try {
				String[] dim_fct = line.split("\t");
				
				StringBuilder sb = new StringBuilder();
				{
					boolean foundit = false;
					
					Map<String, String> dimmap = BmUtil.getParseMap(dim_fct[0]);
					
					// TODO: moving forward, we want to ALWAYS have this field set,
					// but to preserve backwards compat, we'll check here.
					String atype = dimmap.get("aggtype");
					// Util.massert(atype != null, "TODO take this out in general");
					if(atype != null && !(atype.equals(AggType.ad_general.toString())))
						{ continue; }
					
					for(DimCode dimc : dimSet)
					{
						String dimkey = dimc.toString();
						Util.massert(dimmap.containsKey(dimkey), "Dimension key not found: %s, map is %s", dimkey, dimmap);
						String relval = dimmap.get(dimkey);
						
						if("date".equals(dimkey))
						{
							foundit = true;
							
							// Can use just a typical day code
							relval = dayCode;
							
							// relval = "" + Util.dayCode2Int(dayCode);
						}
						
						//relval = "date".equals(dimkey) ? "" + Util.dayCode2Int(relval) : relval;
						sb.append(relval).append("\t");
						
						if(dimc == DimCode.campaign)
						{
							int cid = Integer.valueOf(relval);	
							campIdSet.add(cid);
						}
					}
					
					Util.massert(foundit);
				}
				
				// Okay, for FACTS, we should have ALL the possible values in the output data, since there
				// is nothing to be gained by not including a fact (in contrast to a dimension, where 
				// dropping a dimension decreases the number of output rows).
				// So we should be guaranteed thta fctmap.containsKey(k) for all IntFact and DblFacts. 
				// We might not want to actually put all the Facts in the database, but that's another story
				{
					Map<String, String> fctmap = BmUtil.getParseMap(dim_fct[1]);
					
					for(IntFact ikey : intSet)
					{
						if(ikey != IntFact.bids)
							{ Util.massert(fctmap.containsKey(ikey.toString()), "IntFact not present %s", ikey); }
						
						String relval = fctmap.containsKey(ikey.toString()) ? fctmap.get(ikey.toString()) : "0";
						sb.append(relval).append("\t");
					}
					
					for(DblFact dkey : dblSet)
					{
						Util.massert(fctmap.containsKey(dkey.toString()), "DblFact not present %s", dkey);					
						sb.append(fctmap.get(dkey.toString())).append("\t");
					}
				}
				
				sb.append(dayCode);
				outputLine(sb.toString());

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}
}
