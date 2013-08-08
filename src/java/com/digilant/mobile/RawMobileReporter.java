package com.digilant.mobile;

import java.sql.SQLException;
import java.util.LinkedHashMap;

import com.digilant.fastetl.FileManager;

public class RawMobileReporter {
	private FileManager _fileman;
	private String machine;
	private String db;
	private String dest_table;
	private static  LinkedHashMap<String, String>  colnames_and_types;

public RawMobileReporter(String machine, String db, String dest_table, FileManager f){
	_fileman = f;
	this.machine = machine;
	this.db = db;
	this.dest_table = dest_table;
	try {
		colnames_and_types = DBConnection.lookupColumnsAndTypes(machine, db, dest_table);

	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}
}
