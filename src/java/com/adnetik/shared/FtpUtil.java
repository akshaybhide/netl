
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.net.*;
import java.util.regex.*;
import java.util.zip.GZIPInputStream;

import java.text.SimpleDateFormat;

import com.adnetik.shared.BidLogEntry.*;

public class FtpUtil
{	
	private String userName; 
	private String hostName;
	
	public static final String IOW_KEY_PATH = "/local/fellowship/private_keys/krishna_sftp_backend1.adnetik.iponweb.net--id_rsa";
	public static final String BURFOOT_KEY_PATH = "/home/burfoot/.ssh/priv_key_342";

	private File _keyFile;
	
	private Map<String, String> _loc2Rem = Util.treemap();

	public FtpUtil(String uname, String hname, File kfile)
	{
		userName = uname;
		hostName = hname;
		
		Util.massert(kfile.exists() && !kfile.isDirectory(),
			"Key file %s does not exist or is directory", kfile.getAbsolutePath());
		
		_keyFile = kfile;
	}
	
	public String getSysCall()
	{
		return Util.sprintf("sftp -i %s %s@%s", _keyFile.getAbsolutePath(), userName, hostName);	
	}
	
	public void addPut(String locpath, String rempath)
	{
		_loc2Rem.put(locpath, rempath);
	}
	
	public void doTransfer(String locpath, String rempath) throws Exception
	{
		List<String> oplist = Util.vector();		
		oplist.add(Util.sprintf("put %s %s", locpath, rempath));
		
		Util.pf("%s", oplist.get(0));
		
		// There are some issues with collecting output from transfers
		// Instead, ignore output here, and use check4File to make sure it worked
		List<String> output = runOp(oplist, false);
		
		if(!check4File(rempath))
		{
			throw new Exception("Failed to upload file to remote path " + rempath);
			
		}
	}
	
	public boolean check4File(String remotepath) throws Exception
	{
		List<String> input = Util.vector();
		int lastslash = remotepath.lastIndexOf("/");
		String remdir = remotepath.substring(0, lastslash);
		
		// Util.pf("remote directory is %s\n", remdir);
		input.add(Util.sprintf("ls %s", remdir));
		
		// String fullpath = Util.sprintf("%s/%s", remdir, simplename);
		List<String> output = runOp(input, true);
		for(String oneline : output)
		{
			// Util.pf("One line is %s, fullpath is %s\n", oneline, fullpath);
			if(oneline.indexOf(remotepath) > -1)
			{
				// Util.pf("Found file %s\n", remotepath);
				return true; 
			}
		}
		return false;
	}
	
	private List<String> runOp(List<String> input, boolean collectOutput) throws Exception
	{
		String ftpcall = getSysCall();
		// Util.pf("SYScall is %s\n", ftpcall);
		
		Process p = Runtime.getRuntime().exec(ftpcall);	
		PrintWriter sendto = new PrintWriter(p.getOutputStream());		
		
		for(String oneline : input)
		{
			// Util.pf("Sending input line: %s\n", oneline);
			sendto.write(oneline);
			sendto.write("\n");
		}
		
		sendto.close();	
		
		p.waitFor();
		
		/*
		{
			boolean hiterr = false;
			BufferedReader readerr = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			for(String oneline = readerr.readLine(); oneline != null; oneline = readerr.readLine())
			{
				if(oneline.indexOf("Connected") > -1)
					{ continue; }
				
				Util.pf("ERROR: %s\n", oneline);
				hiterr = true;
			}
			readerr.close();		
		
			if(hiterr)
				{ throw new RuntimeException("Error in FTP operation"); }
		}		
		*/
		
		List<String> output = Util.vector();
		if(collectOutput)
		{
			BufferedReader bread = new BufferedReader(new InputStreamReader(p.getInputStream()));
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				String s = oneline.trim();
				Util.pf("OUtput is %s\n", s);
				
				output.add(oneline.trim());
			}
			bread.close();		
		}
		
		return output;
	}
	
	public static FtpUtil getIowConnection()
	{
		return new FtpUtil("krishna", "ftp.adnetik.iponweb.net", new File(IOW_KEY_PATH));
	}
	
	public static FtpUtil getIowListConn()
	{
		return new FtpUtil("lists", "ftp.adnetik.iponweb.net", new File(BURFOOT_KEY_PATH));
	}
}

