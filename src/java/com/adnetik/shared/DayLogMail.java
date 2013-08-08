
package com.adnetik.shared;

import java.util.*;
import java.io.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.Util.*;


// This class both sends out an email, and writes the log lines to a 
// standard location.
public class DayLogMail extends SimpleMail
{
	String _className;
	
	String _dayCode; 
	
	String _logPath;
	
	public DayLogMail(Object cnameObj, String daycode)
	{
		super(getMailSubject(cnameObj, daycode));
		
		TimeUtil.assertValidDayCode(daycode);
		
		_dayCode = daycode;
		_className = cnameObj.getClass().getSimpleName();
		
		setLogPath();
		
		Util.pf("Log mail is %s\n", _logPath);
		
		FileUtils.createDirForPath(_logPath);
	}
	
	private static String getMailSubject(Object classObj, String daycode)
	{
		String cname = classObj.getClass().getSimpleName();
		cname = cname.endsWith("Report") ? cname : cname+"Report";
		return Util.sprintf("%s for %s", cname, daycode);
	}
	
	
	// Append and close 
	protected void addMessageLine(String oneline)
	{
		super.addMessageLine(oneline);
		
		try {
			FileWriter fwrite = new FileWriter(_logPath, true);
			fwrite.write(oneline);
			fwrite.write("\n");
			fwrite.close();
		} catch (IOException ioex) {
			
			// Nothing smart to do here
			ioex.printStackTrace();	
			
		}
	}
	
	private void setLogPath()
	{
		for(int atid = 0; atid < 100; atid++)
		{
			String probepath = getLogPathAttempt(_className, _dayCode, atid);
			File probe = new File(probepath);
			
			if(!probe.exists())
			{
				_logPath = probepath;
				return;
			}
		}
		
		Util.massert(false, "Could not find a valid probe path after 100 attempts, cname=%s, daycode=%s",
			_className, _dayCode);
	}
	
	
	public String getDayCode()
	{
		return _dayCode;	
	}
	
	private static String getLogPathAttempt(String cname, String daycode, int atid)
	{
		String suff = (atid == 0 ? "" : "__" + atid);
		String username = Util.getUserName();
		return Util.sprintf("/home/%s/daylogs/%s/%s%s.txt", username, daycode, cname, suff);
	}
}

