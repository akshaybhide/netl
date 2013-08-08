
package com.adnetik.adhoc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.*; 
import java.io.*;
import java.util.*;
import java.text.*;


import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

public class ActLog
{
	protected static Connection con;
	protected static int bin0=0,bin1=0,bin2=0,bin3=0,bin4=0,bin5=0,bin6=0,bin7=0,bin8=0,bin9=0;
	public static void main(String[] args) throws Exception
	{
		if(args.length<2)
		{
			System.out.println("input date (yyyy-mm-dd) and the number of days to look back \n");
		 	System.exit(1);
		}
		String datetime=args[0];
		int lookback=Integer.parseInt(args[1]);
		wrapper(datetime,lookback);
				
	}
       private static void wrapper(String date,int lookback) throws SQLException
	{
		con=myconnection();
		try{
 			 transfer(date,lookback);
			}catch(IOException e){
			  e.printStackTrace();
				}
		con.close();
		
	}
       private static void transfer(String date,int lookback) throws IOException
	{
	       
		GregorianCalendar cal = (GregorianCalendar)getCalendar4Date(date);
		GregorianCalendar past = (GregorianCalendar) cal.clone();
		cal.add(Calendar.DATE, 1);
		past.add(Calendar.DATE, -lookback +1);
		ArrayList<String> dayrange = new ArrayList<String>();
		while(past.before(cal)){
			dayrange.add(getString4Calendar(past));
			past.add(Calendar.DATE, 1);
		}
		
		for(String oneday : dayrange)
			{readlog(oneday);Util.pf("daylist %s\n",oneday);}
		
	}
	public static String getString4Calendar(Calendar cal){
		GregorianCalendar calendar = (GregorianCalendar)cal;
		String strmonth = ((calendar.get(Calendar.MONTH)+ 1) < 10)?"0"+(calendar.get(Calendar.MONTH)+ 1):""+(calendar.get(Calendar.MONTH)+ 1);
		String strday = (calendar.get(Calendar.DATE) < 10)?"0"+calendar.get(Calendar.DATE):""+calendar.get(Calendar.DATE);
		String strdate = ""+ calendar.get(Calendar.YEAR) +"-" + strmonth +"-" + strday;	
		return strdate;
	}

	public static Calendar getCalendar4Date(String date){
		String[] parts = date.split("-");
		GregorianCalendar cal = new GregorianCalendar();
		cal.set(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])-1, Integer.parseInt(parts[2]), 1, 1, 1);
		return cal;
	}


       private static void readlog(String datetime) throws IOException
	{

		List<String> mylist=Util.vector();
		
			for(ExcName excname : ExcName.values())

			{	
			
				mylist = Util.getNfsLogPaths(excname, LogType.activity, datetime);

				if(mylist==null) continue;
		
				for(String onepath : mylist)
			      {
			// PathInfo pathinfo = new PathInfo(onepath);
			
	
			// Util.pf("One path is %s\n", onepath);
			
			// Util.pf("Path info is %s\n", pathinfo);
                     
				checkPath(onepath);
			
				// break;
			       }
			}
		
		// checkLogInfo();
	}
	
	private static void checkLogInfo()
	{
		Map<LogField, Integer> convmap = FieldLookup.getFieldMap(LogType.conversion, LogVersion.v21); 	
		Map<LogField, Integer> actvmap = FieldLookup.getFieldMap(LogType.activity  , LogVersion.v21); 		
		
		for(LogField lf : convmap.keySet())
		{
			Util.massert(actvmap.containsKey(lf),
				"LogField %s not found in actvmap", lf);
			
			int a = convmap.get(lf);
			int b = actvmap.get(lf);
			
			Util.pf("LF=%s, conv=%d, actv=%d\n", lf, convmap.get(lf), actvmap.get(lf));
			
			Util.massert(a == b,
				"LF %s is in %d for conv map but %d for actv map",
				lf, convmap.get(lf), actvmap.get(lf));
		}
	}
	
	private static void checkPath(String onepath) throws IOException
	{
		
		PathInfo pathinfo = new PathInfo(onepath);
		
		BufferedReader bread = FileUtils.getGzipReader(onepath);
		String query;
		
              //**********************************************************
                

		try{
			Statement statement = con.createStatement();
                   statement.setFetchSize(1000);
			StringBuffer str = new StringBuffer ();
		       String from=new String();
                   	str.append("INSERT INTO fast_activity_stage (id_campaign,id_date,id_hour,id_lineitem,id_creative,id_exchange,id_country,id_currcode,id_content,id_utw,id_metrocode,id_region,id_browser,id_visibility,id_size,id_language,bin0,bin1,bin2,bin3,bin4,bin5,bin6,bin7,bin8,bin9) VALUES");
                  	
              //***********************************************************
		int test=0; int i=0;
		for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
		{ 	
			bin0=bin1=bin2=bin3=bin4=bin5=bin6=bin7=bin8=bin9=0;
			try { 
				BidLogEntry ble = new BidLogEntry(LogType.activity, pathinfo.pVers, oneline);

				int pixid=ble.getIntField(LogField.conversion_id);
				query="select bin from activity_pixel_bin where activity_pixel_id="+pixid;
				if (querycat(query,statement)!=null)
				{int bin=Integer.parseInt(querycat(query,statement));// Util.pf("%d**",bin);
				     bincast(bin);
				//Util.pf("%d,%d,%d,%d,%d,%d,%d,%d,%d,%d**",bin0,bin1,bin2,bin3,bin4,bin5,bin6,bin7,bin8,bin9);
				}
                            else {Util.pf("!!!%d!!!!",pixid); continue;}

				String date=ble.getField(LogField.date_time);// Util.pf(date);
				date=date.substring(0,10); 
				// BidLogEntry ble = new BidLogEntry(pathinfo.pType, pathinfo.pVers, oneline);
				String campid = ble.getField(LogField.campaign_id);
				String wtpid = ble.getField(LogField.wtp_user_id);
				String hour=ble.getField(LogField.hour);
				String lineitem = ble.getField(LogField.line_item_id);
				String creative=ble.getField(LogField.creative_id);
				String exchange=ble.getField(LogField.ad_exchange);
				query="select id from cat_exchange where code="+"'"+exchange+"'";
				exchange=querycat(query,statement);
				String country=ble.getField(LogField.user_country);
				query="select id from cat_country where code="+"'"+country+"'";
				country=querycat(query,statement); 
				String currency=ble.getField(LogField.currency); 
				query="select id from cat_currcode where code="+"'"+currency+"'"+" "+"order by id desc limit 1";
				String currcode=querycat(query,statement);
				String content=ble.getField(LogField.content);
				query="select id from cat_content where code="+"'"+content+"'";
				content=querycat(query,statement);
				String utw=ble.getField(LogField.utw);
				String metrocode=ble.getField(LogField.user_DMA);// can't find this field!!!!!!!!!!!!!!!!!!!!!!!!
				String region=ble.getField(LogField.region);
				query="select id from cat_region where code="+"'"+region+"'";
				region=querycat(query,statement);
				String browser=ble.getField(LogField.browser);
				query="select id from cat_browser where name="+"'"+browser+"'";
				browser=querycat(query,statement);
				String visibility=ble.getField(LogField.visibility);
				query="select id from cat_visibility where name="+"'"+visibility+"'";
				visibility=querycat(query,statement);
				String size=ble.getField(LogField.size);
				query="select id from cat_size where name="+"'"+size+"'";
				size=querycat(query,statement);	
				String language=ble.getField(LogField.language);	
				query="select id from cat_language where code="+"'"+language+"'";
				language=querycat(query,statement);			
				
				 String binlist=Integer.toString(bin0)+","+Integer.toString(bin1)+","+Integer.toString(bin2)+","+Integer.toString(bin3)+","+Integer.toString(bin4)+","+Integer.toString(bin5)+","+Integer.toString(bin6)+","+Integer.toString(bin7)+","+Integer.toString(bin8)+","+Integer.toString(bin9);
			//	Util.pf(binlist);			
			//	Util.pf("Campaign ID = %d, creativeid=%d,exchange=%d,country=%d,currcode=%d,utw=d%,metro=d%,region=d%,brow=d%,visi=d%,size=d%,lang=d%\n", campid, creative,ex,country,curr,utw,metro,region,brow,visi,size,lang);
			//********************************************************************

			
                      if (i==0)
                      {
                          from="("+campid+","+"'"+date+"'"+","+hour+","+lineitem+","+creative+","+exchange+","+country+","+currcode+","+content+","+utw+","+metrocode+","+region+","+browser+","+visibility+","+size+","+language+","+binlist+")";
                          i=1;
                      }
                      else 
                      {
                          from=","+"("+campid+","+"'"+date+"'"+","+hour+","+lineitem+","+creative+","+exchange+","+country+","+currcode+","+content+","+utw+","+metrocode+","+region+","+browser+","+visibility+","+size+","+language+","+binlist+")";
;   
                      }

                     str.append(from); 
                     if (str.length()>1000) // when the string length is greater than 1000, execute insert statement, i.e., insert 1000 records each time
                     {	
                        // System.out.print(str);
                         statement.executeUpdate(str.toString());
                         str = new StringBuffer ();
                         str.append("INSERT INTO fast_activity_stage (id_campaign,id_date,id_hour,id_lineitem,id_creative,id_exchange,id_country,id_currcode,id_content,id_utw,id_metrocode,id_region,id_browser,id_visibility,id_size,id_language,bin0,bin1,bin2,bin3,bin4,bin5,bin6,bin7,bin8,bin9) VALUES");
                         i=0;
                     } 
			
		  }   		
		catch (BidLogFormatException blex) {
			
			blex.printStackTrace();	
		}
		
          			if(i==1)  // Insert the rest of the string buffer 
                		statement.executeUpdate(str.toString());
		}   
	  		 bread.close();
	   		 statement.close();

			//*********************************************************************
		   }
	         catch ( SQLException e )   
                {   
                    System.out.println( "JDBC error: " + e );  
                    System.out.println("Table already exists!");
                }   
/*                finally  
                {   
                    con.close();   
                }   */

             }  

	
  
       public static Connection myconnection() throws SQLException{
			// DbUtil.doClassInit();
			//Class.forName("com.mysql.jdbc.Driver").newInstance();
			Connection con = DriverManager.getConnection( "jdbc:mysql://thorin-internal.digilant.com/fastetl", "reporting", "reporting_101?" );
			return con;

			}       

      private static String querycat(String query,Statement st) throws SQLException{
                   ResultSet rs = st.executeQuery(query);
                   String seg1=null;
                    while (rs.next()) { 
                    seg1=rs.getString(1);
			}
		     return seg1;
		}
      private static void bincast(int bin) {
		if(bin==1)
		bin1=1;
              else if(bin==2) bin2=1;
              else if(bin==3) bin3=1;
              else if(bin==4) bin4=1;
              else if(bin==5) bin5=1;
              else if(bin==6) bin6=1;
              else if(bin==7) bin7=1;
              else if(bin==8) bin8=1;
              else if(bin==9) bin9=1;
              else if(bin==0) bin0=1;
		}
             
}
