
package com.digilant.fastetl;

import java.util.*;

import com.adnetik.shared.PathInfo;
import com.adnetik.shared.Util;
import com.adnetik.shared.Util.*;
import com.digilant.fastetl.FastUtil.MyLogType;

public class MyPathInfo
{
	public MyLogType pType;
	public LogVersion pVers;
	
	public DataCenter pCenter;
	public ExcName pExc;
	
	public MyPathInfo(String fullpath)
	{
		String simpname = Util.lastToken(fullpath, "/");
		
		//Util.pf("\nSimpname is %s, toks is %d", simpname, simpname.split("\\.").length);
		
		String[] subtoks = simpname.split("\\.");
		
		// logtype_version
		
		String ltv = subtoks[2];
		if(ltv.contains("pixel")){
			pVers = LogVersion.v12;
			
			pType = MyLogType.valueOf(MyLogType.pixel.toString());
			
			
		}
		else{
			int flen = ltv.length();
			pVers = LogVersion.valueOf(ltv.substring(flen-3, flen));
			pType = MyLogType.valueOf(ltv.substring(0, flen-4));
			
			
			// datacenter
			{
				String targs = subtoks[3];
				
				// TODO: this is fucked up
				if(targs.startsWith("google"))
					{ pExc = ExcName.rtb; }
				
				for(int i = 0; pExc == null && i < ExcName.values().length; i++)
				{
					if(targs.startsWith(ExcName.values()[i].toString()))
						{ pExc = ExcName.values()[i]; }
				}
				
				for(DataCenter dc : DataCenter.values())
				{
					if(targs.indexOf(dc.toString()) > -1)
						{ pCenter = dc; }
				}
			}
			
		}
	}
	
	public String toString()
	{
		return Util.sprintf("logtype=%s\tlogversion=%s\tdatacenter=%s\texcname=%s", pType, pVers, pCenter, pExc);	
		
	}
	
	public static void main(String[] args)
	{
		Scanner sc = new Scanner(System.in);
		
		while(sc.hasNextLine())
		{
			String path = sc.nextLine();
			PathInfo pi = new PathInfo(path);
			
			Util.pf("\nPathInfo is %s", pi.toString());
			
			
		}
		
	}
}

