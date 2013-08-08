
package com.adnetik.adhoc;

import com.jcraft.jsch.*;

public class JschTest {
	public static void main(String args[]) {
		JSch jsch = new JSch();
		Session session = null;
		try {
			session = jsch.getSession("digilant_OUT", "ftp.lscaccess1.net", 22);
			session.setConfig("StrictHostKeyChecking", "no");
			session.setPassword("digLABAS123");
			session.connect();
			
			Channel channel = session.openChannel("sftp");
			channel.connect();
			ChannelSftp sftpChannel = (ChannelSftp) channel;
			sftpChannel.put("test1.txt", "test1.txt");
			sftpChannel.exit();
			session.disconnect();
		} catch (JSchException e) {
			e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
		} catch (SftpException e) {
			e.printStackTrace();
		}
	}
}

