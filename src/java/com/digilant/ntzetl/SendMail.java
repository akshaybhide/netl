
package com.digilant.ntzetl;

import java.util.*;
import java.io.*;

import java.text.SimpleDateFormat;

import com.adnetik.shared.Util.*;
import com.adnetik.shared.*;

public class SendMail
{

	
	public static final List<String> ADMIN_LIST = Util.vector();
	public static final String DEFAULT_SUBJECT = "TestMail";
	
	public static final int MAX_LINES = 10000;
	
	public String subject;
	public List<String> messgList = Util.vector();
	public boolean useDate; 
	
	boolean _print2console = true;
	
	static {
		ADMIN_LIST.add("huimin.ye@digilant.com");
		//ADMIN_LIST.add("daniel.burfoot@gmail.com");
		ADMIN_LIST.add("aubrey.jaffer@digilant.com");
	}
	public void renewAdminList(List<String> list){
		ADMIN_LIST.clear();
		ADMIN_LIST.addAll(list);
	}
	public SendMail()
	{
		this(DEFAULT_SUBJECT);
	}

	public SendMail(String subj)
	{
		this(subj, true);
	}
	
	public SendMail(String subj, boolean ud)
	{
		subject = subj;
		useDate = ud;
	}
	
	public static SendMail createFromException(String sysname, Throwable ex)
	{
		SendMail exmail = new SendMail(sysname + " Exception: " + ex.getMessage());
		
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
	
	public void addLogLine(String line)
	{
		String ts = useDate ? myTimestamp() + " : " : "";			
		messgList.add(ts + line);
	}

	public static void sendExceptionMail(String systemcode, Throwable ex)
	{
		SendMail exmail = new SendMail("EXCEPTION ERROR IN " + systemcode);
		exmail.addExceptionData(ex);
		exmail.send2admin();
	}
	
	public void readLogData(String path)
	{
		messgList.addAll(FileUtils.readFileLinesE(path));	
	}
	
	public int numLines()
	{
		return messgList.size();
	}
	
	public void writeLogData(String path)
	{
		// Subject information is lost. Dob't cry over spilled milk.
		List<String> datalines = Util.vector();
		datalines.add("AdminMail log for mail : " + subject);
		datalines.addAll(messgList);
		FileUtils.writeFileLinesE(datalines, path);
	}
	
	public void setPrint2Console(boolean p2c)
	{
		_print2console = false;	
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
			
			for(String m : messgList)
			{
				//Util.pf("\nThe line is '%s' ", m);
				pwrite.printf("\n%s", m); 
			}
			
			pwrite.close();
			
			p.waitFor();
			
			// Util.pf("\nSent message\n");		
		} catch (Exception ex) {
			
			// Don't throw this - don't want an error here to bring down the whole ETL chain
			Util.pf("\nError in SendMail: %s", ex.getMessage());
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


