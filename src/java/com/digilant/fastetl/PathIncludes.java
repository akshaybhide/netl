package com.digilant.fastetl;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;

public class PathIncludes implements FilenameFilter {
	private String[] criteria;
	private String folder;
	public PathIncludes(String foldername, String[] ar ){
		criteria = ar;
		folder = foldername;
	}
	@Override
	public boolean accept(File arg0, String arg1) {
		// TODO Auto-generated method stub
		if(arg1.contains(folder)) return true;
		boolean r = arg0.getAbsolutePath().toLowerCase().contains(folder);
		for(int i = 0; i < criteria.length&& r==true; i++)
			r = r &&   arg1.toLowerCase().contains(criteria[i]);
		return r;
	}

}
