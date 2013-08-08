package com.digilant.ntzetl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

public class pixelParam2BinMapper {
	HashMap<String,NBinArrayList<String>> pixel_param_bin_map =new HashMap<String,NBinArrayList<String>>();
	HashMap<String,ArrayList<String>> pixel_param_map =new HashMap<String,ArrayList<String>>();
	HashSet<String> alreadythere = new HashSet<String>();
    private pixelParam2BinMapper(){
    	LoadPixelParamTable();
    	LoadPixelParamBinTable();
    	//System.out.println(pixel_param_map);
    	System.out.println(pixel_param_bin_map);
    	addNewParams();
    	System.out.println(pixel_param_bin_map);
    	writePixelParamBin2DB();
    	
    } 
    public void writePixelParamBin2DB(){
			try {
				Class.forName("org.netezza.Driver").newInstance();
	    		   Connection con = DriverManager.getConnection( "jdbc:netezza://66.117.49.50/fastetl", "armita", "data_101?" );

	               Statement statement = con.createStatement();

	               statement.setFetchSize(1000);

		    for(String pixel : pixel_param_bin_map.keySet()){
			    for(int i = 0; i < pixel_param_bin_map.get(pixel).size(); i++){
			    	String key = pixel+"_"+pixel_param_bin_map.get(pixel).get(i)+"_"+i;
			    	String query= "insert into pixel_param_bin (comp_key, pixel_id, param_name, bin_no) values ('"  +key+ "', "+ pixel + ",'"+ pixel_param_bin_map.get(pixel).get(i) +"', " +i + ")" ;
			    	if(!alreadythere.contains(key)){
			    		System.out.println(query);
			    		statement.executeUpdate(query);
			    	}
			    }
			   }
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (IllegalAccessException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}catch ( SQLException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
} catch (ClassNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 		}
	public void printParamMap(){
		for(String s : pixel_param_map.keySet()){
			System.out.println(s + " : " + pixel_param_map.get(s).toString());
		}
	}
	public void printParamBinMap(){
		
	}
	public void addNewParams(){
		
		for(String pixel:pixel_param_map.keySet()){
			if(pixel_param_bin_map.containsKey(pixel)){
				for(int i = 0; i < pixel_param_map.get(pixel).size(); i++){
					String param = pixel_param_map.get(pixel).get(i); 
					if(!pixel_param_bin_map.get(pixel).contains(param)){
						if(i < 10)	
						pixel_param_bin_map.get(pixel).add(param);
							
					}
				}
			}else{
				NBinArrayList<String> nb_arr = new NBinArrayList<String>(10);
				nb_arr.addAll(pixel_param_map.get(pixel));
				pixel_param_bin_map.put(pixel, nb_arr);
			}
		}
	}
    public static pixelParam2BinMapper getInstance(){
		pixelParam2BinMapper ins  = new pixelParam2BinMapper();
		return ins;
    }
	public void LoadPixelParamTable(){    
		try {
				Class.forName("org.netezza.Driver").newInstance();
	     		   Connection con = DriverManager.getConnection( "jdbc:netezza://66.117.49.50/adnetik", "armita", "data_101?" );

                   Statement statement = con.createStatement();

                   statement.setFetchSize(1000);

		    int change=0;
		    String query="select pixel_id, name from adnetik.pixel_param  order by pixel_id,  ID desc";
		    ResultSet rs = statement.executeQuery(query);
		    String pixel,paramname;
		    while (rs.next()) {
		    	pixel=rs.getString(1);
		    	paramname=rs.getString(2);
		    	if (pixel_param_map.get(pixel)==null){
				ArrayList<String> tmp = new ArrayList<String>();
				tmp.add(paramname);
				pixel_param_map.put(pixel, tmp);
				}
		    	else
		    		if(!pixel_param_map.get(pixel).contains(paramname))
		    			pixel_param_map.get(pixel).add(paramname);
		    }
   	
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
	public String getBin(String pixel, String paramname){
		if(pixel_param_bin_map.get(pixel).contains(paramname))
		return "param"+pixel_param_bin_map.get(pixel).indexOf(paramname);
		return "";
	}

	public void LoadPixelParamBinTable(){    
		try {
				Class.forName("org.netezza.Driver").newInstance();
	     		   Connection con = DriverManager.getConnection( "jdbc:netezza://66.117.49.50/fastetl", "armita", "data_101?" );

                   Statement statement = con.createStatement();

                   statement.setFetchSize(1000);

		    int change=0;
		    String query="select pixel_id, param_name, bin_no, comp_key from fastetl.pixel_param_bin order by pixel_id, bin_no";
		    ResultSet rs = statement.executeQuery(query);
		    String pixel,paramname, bin, key;
		    while (rs.next()) {
		    	pixel=rs.getString(1);
		    	paramname=rs.getString(2);
		    	bin=rs.getString(3);
		    	key = rs.getString(4);
		    	alreadythere.add(key);
		    	if (pixel_param_bin_map.get(pixel)==null){
		    		NBinArrayList<String> nblist = new NBinArrayList<String>(10);
		    		nblist.add(paramname);
		    		pixel_param_bin_map.put(pixel, nblist);
				}
		    	else{
		    		int idx = pixel_param_bin_map.get(pixel).indexOf(paramname);
		    		if( idx == -1)
		    		pixel_param_bin_map.get(pixel).add(paramname);
		    	}
		    }
//		    System.out.println(pixel_param_bin_map);
   	
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }

	public static void main( String[] args ) {
		pixelParam2BinMapper map = pixelParam2BinMapper.getInstance();
		//map.printParamMap();
	}

}
