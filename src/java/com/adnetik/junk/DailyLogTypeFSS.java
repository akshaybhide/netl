
package com.adnetik.bm_etl;

import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;

public class DailyLogTypeFSS
{
	
	// TODO: get rid of this whole class
	List<String> pathlist = Util.vector();
	LogType relType;
	String dayCode;

	public DailyLogTypeFSS(LogType rtype)
	{
		this(rtype, TimeUtil.getYesterdayCode());	
	}
	
	public List<String> getPathList() { return pathlist; }
	
	public DailyLogTypeFSS(LogType rtype, String dcode)
	{
		relType = rtype;
		dayCode = dcode;
		
		for(ExcName exc : ExcName.values())
		{
			List<String> exlist = Util.getNfsLogPaths(exc, relType, dayCode);
			
			if(exlist != null)
			{
				for(String onepath : exlist)
					{ pathlist.add("file://" + onepath); }
			}
		}
		
		Util.pf("\nFound %d total files for logtype=%s, daycode=%s", pathlist.size(), relType, dayCode);
	}

	public int getMaxFileSetId()  { return pathlist.size(); }
	
	public String getFileSetPath(int fileSetId)
	{
		return pathlist.get(fileSetId);
	}

	public Integer createFileSet(String processName)
	{
		throw new RuntimeException("not yet implemented");
		
	}

	public List<Integer> getFileSetsNotProcessed(String sourceProcess,String processName)
	{
		List<Integer> x = Util.vector();
		
		for(int i = 0; i < pathlist.size(); i++)
			{ x.add(i); }
		
		return x;		
	}

	public void jobFinished(String hdfsTemporaryOutputPath, List<Integer> processedFileSets, String processName)
	{
		throw new RuntimeException("not yet implemented");
	}

	public void loaderFinished(List<Integer> processedFileSets, String processName)
	{
		throw new RuntimeException("not yet implemented");
	}
}
