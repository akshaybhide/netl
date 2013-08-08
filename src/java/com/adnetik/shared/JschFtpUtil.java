
package com.adnetik.shared;

import java.io.*;
import java.util.*;

import com.jcraft.jsch.*;

public class JschFtpUtil {
	
	protected String _userName;
	protected String _hostName;
	protected String _passWord;
	
	protected ChannelSftp _myChannel;
	protected Session _mySession;
	
	public JschFtpUtil(String host, String user, String pass)
	{
		_hostName = host;
		_userName = user;
		_passWord = pass;
	}
	
	protected void setupSession() throws SftpException,JSchException
	{
		JSch jsch = new JSch();
		_mySession = jsch.getSession(_userName, _hostName, 22);
		_mySession.setConfig("StrictHostKeyChecking", "no");
		_mySession.setPassword(_passWord);
		_mySession.connect();
		
		_myChannel = (ChannelSftp) _mySession.openChannel("sftp");
		_myChannel.connect();
	}
	

	
	public static IowListFtp getIowListFtp() throws SftpException,JSchException
	{
		return new IowListFtp("ftp.adnetik.iponweb.net", "lists");
	}
	
	private void endSession() throws SftpException,JSchException
	{
		_myChannel.disconnect();
		_mySession.disconnect();		
	}
	
	public void doTransfer(String locpath, String rempath)
	{
		putFile(new File(locpath), rempath);	
	}
	
	public void putFile(File local, String dst)
	{
		Util.massert(local.exists(), "Local file %s not found", local.getAbsolutePath());
		
		//Util.pf("Uploading file %s to %s\n",
		//	local.getAbsolutePath(), dst);
		
		try {
			setupSession();
			putFileSub(local, dst);
			endSession();
		} catch (Exception ex) {
			throw new RuntimeException(ex);	
		}
	}
	
	public boolean createDir(String remotedir)
	{
		try {
			setupSession();
			createDirSub(remotedir);
			endSession();
			return true;
			
		} catch (Exception ex) {
			
			if(ex.getMessage().indexOf("MKD-exists") > -1)
				{ return false; }
			
			throw new RuntimeException(ex);	
		}		
	}
	
	private void createDirSub(String remotedir) throws SftpException,JSchException
	{	
		_myChannel.mkdir(remotedir);	
	}	
	
	private void putFileSub(File local, String dst) throws SftpException,JSchException
	{	
		_myChannel.put(local.getAbsolutePath(), dst);		
	}
	
	public boolean check4File(String remotepath) throws Exception
	{
		setupSession();
		
		List<String> input = Util.vector();
		int lastslash = remotepath.lastIndexOf("/");
		String remdir = remotepath.substring(0, lastslash);
		String shortname = remotepath.substring(lastslash+1);
			
		// Util.pf("Remote directory is %s, shortname is %s\n", remdir, shortname);
		
		Vector myvect = _myChannel.ls(remdir);
	
		// if(myvect.
		
		boolean found = false; 
		
		for(Object onelist : myvect)
		{
			ChannelSftp.LsEntry  lsent = Util.cast(onelist);
			
			// Util.pf("Response is %s\n", lsent.getFilename());
			
			if(lsent.getFilename().equals(shortname))
			{ 
				found = true; 
				break;
			}
			
		}
	
		endSession();
		
		return found;
		
	}
	
	public static class IowListFtp extends JschFtpUtil
	{
		
		public IowListFtp(String hostname, String username)
		{
			super(hostname, username, null);	
		}
		
		@Override
		protected void setupSession() throws SftpException,JSchException
		{
			JSch jsch = new JSch();
			jsch.addIdentity(FtpUtil.BURFOOT_KEY_PATH);		
			
			_mySession = jsch.getSession(_userName, _hostName, 22);
			_mySession.setConfig("StrictHostKeyChecking", "no");
			
			_mySession.connect();
			
			_myChannel = (ChannelSftp) _mySession.openChannel("sftp");
			_myChannel.connect();	
		}		
		
	}
	
	
	// public void putFile(File localfile
	
	public static void main(String args[])  throws Exception
	{
		JschFtpUtil jfu = getIowListFtp();
		
		// File locfile = new File("teststatus.xml");
		
		// jfu.putFile(locfile, "/wtpcookie/teststatus.xml");
		
		String s = (jfu.check4File("/wtpcookie/teststatu.xml") ? "FOUND" : "NOT FOUND");
		
		Util.pf("File is %s\n", s);
	}
}

