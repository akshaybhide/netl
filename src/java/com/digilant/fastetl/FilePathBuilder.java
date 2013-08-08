package com.digilant.fastetl;

import com.adnetik.shared.Util;

public class FilePathBuilder {
	FilePathBuilder(String base){
		_strBaseFolder = base;
	}
	private String _strBaseFolder;
	private String _strLineitemId;  
	public String MakePath(){
		return _strBaseFolder;
	}
}
