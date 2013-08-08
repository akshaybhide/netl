

package com.adnetik.bm_etl;

import java.sql.*;
import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util;
import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import java.math.BigInteger;

public class RmxAlert
{

	public static void main(String[] args)
	{
		doAlert();
	}

	
	public static void doAlert()
	{
		SimpleMail logmail = new SimpleMail("Unmapped RMX external line items");
		
		try {
			Connection conn = DatabaseBridge.getDbConnection(DbTarget.external);
			
			String probesql = "select distinct(ELI.name) from bm_etl.rmx_data_perm RDP inner join adnetik.external_line_item ELI on  RDP.external_line_item_id = ELI.id where RDP.campaign_id IS NULL";
			// String probesql = "select distinct(external_line_item_id) as exlid from rmx_data_perm where campaign_id is null order by exlid";
			List<String> emptylist = DbUtil.execSqlQuery( probesql, conn);
			conn.close();
			
			logmail.pf("Found %d empty RMX items\n", emptylist.size());
			
			for(String emptyname : emptylist)
			{
				logmail.pf("RMX External line item %s is unmapped\n", emptyname);
			}
			
			// I was going to say, only send if there are unmapped line items, 
			// but it's probably worth it to just send out an alert that says zero-unmapped
			{
				List<String> recplist = Util.vector();
				recplist.add("raj.joshi@digilant.com");
				recplist.add("krishna.boppana@digilant.com");
				recplist.add("john.hamilton@digilant.com");
				recplist.add("daniel.burfoot@digilant.com");
				logmail.send(recplist);
			}			
			
			
		} catch (SQLException sqlex) {
			
			throw new RuntimeException(sqlex);
		}
	}
}
