package com.digilant.mobile;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Set;


public class DBHelper {
	ArrayList<String> data;
	DBConnection db;
	/**
	 * @param args
	 * @throws FileNotFoundException 
	 * @throws SQLException 
	 */
	public static void main(String[] args) throws FileNotFoundException, SQLException {
		// TODO Auto-generated method stub
		DBHelper dbh = new DBHelper();
		//dbh.readfile("data.txt");
		//dbh.PopulateTable("thorin-internal.adnetik.com", "mobile", "mobile_dimensions", new ArrayList<String>(Arrays.asList("column_name", "logfield_name","machine", "db","query")));
	}
	public DBHelper(){
		db = new DBConnection();
		data = new ArrayList<String>();
	}
	void readfile(String filepath) throws FileNotFoundException{
		
		Scanner sc = new Scanner(new File(filepath));
		while(sc.hasNext())
		{
			data.add(sc.nextLine());
		}
		sc.close();				

	}
	public void PopulateTable(String machine, String db, String tablename, ArrayList<String> colnames) throws SQLException{
		Connection con  = DBConnection.getConnection(machine, db);
		String cols = colnames.toString();
		cols = cols.replace("[", "(");
		cols = cols.replace("]", ")");
		for(String val : data){
			val = val.replaceAll(",","','");
			String query = "replace into "+ tablename + " " +cols + " values ('" + val + "')";
			System.out.println(query);
			DBConnection.runBatchQuery(con, query);
		}
		con.close();
		/*int i = 0;
		for(String key : keys){
			vals.add(data.get(key));
			if(i > 0)
				query+=",";
			query+= key;
		}

		for(String key : keys){
			vals.add(data.get(key));
			if(i > 0)
				query+=",";
			query+= key;
		}
		query+=") values (";
		int i = 0;
		for(String val : vals){
			if(i > 0)
				query+=",";
			query+= val;
			
		}*/
	}
}
