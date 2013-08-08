
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;

public class AdbListQuery
{		
	XmlTree _xTree; 
	
	public static void main(String[] args)
	{	
		AdbListQuery alq = new AdbListQuery();
		alq.writeWithListIndex();
		
		
		/*
		List<String> idlist = alq.getIdList();
		Set<String> probset = getProblemSet();
		
		for(String probid : probset)
		{
			boolean found = false;
			
			for(int i = 0; i < idlist.size() && !found; i++)
			{
				String oneid = idlist.get(i);
				if(oneid.equals(probid))
				{
					Util.pf("Found the problem ID %s at %d out of %d\n",
						oneid, i, idlist.size());
					
					found = true;
				}
			}		
			
			if(!found) 
			{
				Util.pf("Problem ID %s not found in LIST data\n", probid);	
			}
		}
		*/
	}
	
	void writeWithListIndex()
	{
		int id = 0;
		
		for(XmlTree xtree : _xTree.getChildren())
		{
			xtree.setI("record_id", id++);
		}
		
		_xTree.writeXml("cleanlistwithids.xml");
		
	}
	
	public static Set<String> getProblemSet()
	{
		Set<String> pset = Util.treeset();
		// pset.add("6zj");
		pset.add("3OH");
		pset.add("uma");
		pset.add("Z1X");
		pset.add("CYR");
		pset.add("gsn");
		pset.add("svV");
		pset.add("VZ3");
		pset.add("SmX");
		pset.add("D3e");
		pset.add("fN4");
		return pset;	
	}
		
	public AdbListQuery()
	{
		_xTree = XmlTree.loadFromXml("lists-1366123801.xml");
	}
	
	public List<String> getIdList()
	{
		List<String> idlist = Util.vector();
		
		for(XmlTree onekid : _xTree.getChildren())
		{
			idlist.add(onekid.getAtt("id"));	
		}
		
		return idlist;
	}
	
	public Set<String> getCodeSet()
	{
		Set<String> codeset = Util.treeset();
		
		for(XmlTree onekid : _xTree.getChildren())
		{
			codeset.add(onekid.getCode());	
			
		}
		
		return codeset;
	}
}
