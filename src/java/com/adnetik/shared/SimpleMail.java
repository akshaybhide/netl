
package com.adnetik.shared;

import java.util.*;
import java.io.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.Util.*;

public class SimpleMail
{

	
	private static final List<String> ADMIN_LIST = Util.vector();
	public static final String DEFAULT_SUBJECT = "TestMail";
	
	public static final int MAX_LINES = 10000;
	
	public String subject;
	protected List<String> _messgList = Util.vector();
	
	private boolean _useDate; 
	
	boolean _print2console = true;
	
	static {
		ADMIN_LIST.add("daniel.burfoot@digilant.com");
		// ADMIN_LIST.add("daniel.burfoot@gmail.com");
		ADMIN_LIST.add("david.decker@digilant.com");
		//ADMIN_LIST.add("trevor.blackford@digilant.com");
	}
	
	public static void renewAdminList(List<String> list)
	{
		ADMIN_LIST.clear();
		ADMIN_LIST.addAll(list);
	}

	public SimpleMail()
	{
		this(DEFAULT_SUBJECT);
	}

	public SimpleMail(String subj)
	{
		this(subj, true);
	}
	
	public SimpleMail(String subj, boolean ud)
	{
		subject = subj;
		_useDate = ud;
	}
	
	public static SimpleMail createFromException(String sysname, Throwable ex)
	{
		SimpleMail exmail = new SimpleMail(sysname + " Exception: " + ex.getMessage());
		
		for(StackTraceElement elem : ex.getStackTrace())
			{ exmail.pf("%s\n", elem.toString()); }
		
		return exmail;
	}
	
	private void addExceptionData(Throwable ex, int depth)
	{
		if(depth > 20)
			{ return; }
		
		pf("%s\n", ex.getMessage());
		
		for(StackTraceElement elem : ex.getStackTrace())
			{ pf("%s\n", elem.toString()); }		
		
		if(ex.getCause() != null)
		{
			pf("CAUSED BY:\n");
			addExceptionData(ex.getCause(), depth+1);
		}		
	}
	
	public void addExceptionData(Throwable ex)
	{
		addExceptionData(ex, 0);
	}
	
	public void setUseDate(boolean ud)
	{
		_useDate = ud;
	}
	
	public void addLogLine(String line)
	{
		String ts = _useDate ? myTimestamp() + " : " : "";			
		addMessageLine(ts + line);
	}
	
	protected void addMessageLine(String oneline)
	{
		_messgList.add(oneline);
	}

	public static void sendExceptionMail(String systemcode, Throwable ex)
	{
		SimpleMail exmail = new SimpleMail("EXCEPTION ERROR IN " + systemcode);
		exmail.addExceptionData(ex);
		exmail.send2admin();
	}
	
	public void readLogData(String path)
	{
		for(String oneline : FileUtils.readFileLinesE(path))
			{  addMessageLine(oneline); }
	}
	
	public int numLines()
	{
		return _messgList.size();
	}
	
	public void writeLogData(String path)
	{
		// Subject information is lost. Dob't cry over spilled milk.
		List<String> datalines = Util.vector();
		datalines.add("AdminMail log for mail : " + subject);
		datalines.addAll(_messgList);
		FileUtils.writeFileLinesE(datalines, path);
	}
	
	public void setPrint2Console(boolean p2c)
	{
		_print2console = p2c;	
	}
	
	public void pf(String mline, Object... vargs)
	{
		String m = Util.sprintf(mline, vargs);
		
		if(_print2console)
			{ Util.pf("%s", m); }
		
		addLogLine(m.trim());
	}
	
	public static String myTimestamp()
	{
		SimpleDateFormat sdf = new SimpleDateFormat("MM-dd HH:mm:ss");
		return sdf.format((new GregorianCalendar()).getTime());
	}	
	
	public void logOrSend(String logfile)
	{
		if(logfile != null)
			{ writeLogData(logfile); }
		else 
			{ send2admin(); }
	}
	
	public void send2AdminList(AdminEmail... recplist)
	{
		for(AdminEmail oneadmin : recplist)
			{ send(oneadmin.getEmailAddr()); }
	}
	
	public void send2admin()
	{
		send(ADMIN_LIST);
	}
	
	public void send(List<String> reciplist)
	{
		for(String oner : reciplist)
			{ send(oner); }
	}

	
	public void sendPlusAdmin(String... extras)
	{
		send2admin();
		
		for(String oner : extras)
			{ send(oner); }
	}
	
	public void send(String recip)
	{
		// run the Unix "ps -ef" command
		// using the Runtime exec method:
		//String commline = Util.sprintf("mail -s \"[AdminMail] %s\" %s", sub, recip);
		
		//Util.pf("\nCommand line is %s", commline);
		try {
			String[] cmdargs = new String[] { "mail", "-s", Util.sprintf("[AdminMail] %s", subject), recip };
			
			Process p = Runtime.getRuntime().exec(cmdargs);
			
			PrintWriter pwrite = new PrintWriter(p.getOutputStream());
			
			for(String m : _messgList)
			{
				//Util.pf("\nThe line is '%s' ", m);
				pwrite.printf("\n%s", m); 
			}
			
			pwrite.close();
			
			p.waitFor();
			
			// Util.pf("\nSent message\n");		
		} catch (Exception ex) {
			
			// Don't throw this - don't want an error here to bring down the whole ETL chain
			Util.pf("\nError in SimpleMail: %s", ex.getMessage());
			ex.printStackTrace();
		}
	}	
	
	private static void createException(int d)
	{
		if(d > 5)
		{
			throw new RuntimeException("caused an exception at depth " + d);	
		}
		
		createException(d+1);
	}
	
	public static void main(String[] args)
	{
		try { 
			createException(0);
		} catch (Exception ex) {
			
			sendExceptionMail("TEST EX", ex); 
		}
		
		
	}
}

