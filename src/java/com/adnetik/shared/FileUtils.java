
package com.adnetik.shared;

import java.util.*;
import java.util.regex.*;

import java.util.zip.*;
import java.io.*;
import java.io.*;


import java.nio.channels.FileChannel;

import com.adnetik.shared.Util.*;

public class FileUtils
{
	public static void writeRow(Writer mwrite, String delim, Collection<? extends Object> data) throws IOException
	{
		Util.massert(mwrite != null, "Null writer");
		
		LinkedList<Object> templist = Util.linkedlist();
		templist.addAll(data);
		
		while(!templist.isEmpty())
		{
			Object o = templist.pollFirst();			
			mwrite.write(o == null ? "null" : o.toString());
			mwrite.write(templist.isEmpty() ? "\n" : delim);
		}
	}	
	
	public static void writeRow(Writer mwrite, String delim, Object... rowdata) throws IOException
	{
		for(int i = 0; i < rowdata.length; i++)
		{
			mwrite.write(rowdata[i].toString());
			mwrite.write(i < rowdata.length-1 ? delim : "\n");
		}
	}
	
	
	public static List<String> readNLines(BufferedReader toread, int N)
	throws IOException
	{
		List<String> nlist = Util.vector();
		
		for(String oneline = toread.readLine(); oneline != null && nlist.size() < N; oneline = toread.readLine())
		{
			nlist.add(oneline);	
		}
		
		return nlist;
	}
	
	public static void createDirForPath(String path)
	{
		File dirfile = (new File(path)).getParentFile();
		
		if(!dirfile.exists())
			{ dirfile.mkdirs(); }
	}
	
	public static List<String> readFileLines(String path) throws IOException
	{
		Scanner sc = new Scanner(new File(path));	
		List<String> mylist = Util.vector();
		
		while(sc.hasNextLine())
		{
			String oneline = sc.nextLine();	
			mylist.add(oneline);
		}
		
		sc.close();
		return mylist;
	}
	
	public static List<String> readFileLinesE(String path)
	{
		try { return readFileLines(path); }
		catch (IOException ioex) { throw new RuntimeException(ioex); }
		
	}
	
	public static void writeFileLinesE(Collection<? extends Object> data, String path)
	{
		try { writeFileLines(data, path); }
		catch (IOException ioex) { throw new RuntimeException(ioex); }
	}
	public static void writeFileLines(Collection<? extends Object> data, String path) throws IOException
	{
		PrintWriter pw = new PrintWriter(path);
		
		for(Object o : data)
		{
			pw.printf("%s\n", o);	
		}
		
		pw.close();		
	}
	
	public static boolean pathExists(String fpath)
	{
		File f = new File(fpath);
		return f.exists();
	}
	
	public static void deleteIfExists(String filepath) throws IOException
	{	
		File myfile = new File(filepath);
		
		if(myfile.exists())
			{ myfile.delete(); }		
	}
	
	public static void recursiveDeleteFile(File f) throws IOException
	{
		if (f.isDirectory()) {
			for (File c : f.listFiles()) 
				{ recursiveDeleteFile(c); }
		}
		if (!f.delete())
			throw new FileNotFoundException("Failed to delete file: " + f);
	}	
	
	public static void writeFile(String data, String path)
	{
		try {
			PrintWriter pw = new PrintWriter(path);
			pw.print(data);
			pw.close();
			
			
		} catch (IOException ioe) {
			
			throw new RuntimeException(ioe);	
			
		}
	}
	
	public static void serialize(Serializable tosave, OutputStream out) throws IOException
	{
		ObjectOutputStream oos = new ObjectOutputStream(out); 
		oos.writeObject(tosave);
		oos.close();		
	}
	
	public static void serialize(Serializable tosave, String path) throws IOException
	{
		serialize(tosave, new FileOutputStream(path));
	}
	
	public static void serializeEat(Serializable tosave, String path)
	{
		try { serialize(tosave, path); }
		catch (IOException ioex)  {
			throw new RuntimeException(ioex);	
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T unserializeEat(String path) 
	{
		try { 
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path));
			T theObj = (T) ois.readObject();
			ois.close();
			
			return theObj;
		
		} catch(Exception ex) {
			throw new RuntimeException(ex);	
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T unserialize(String path) throws Exception
	{
		return (T)unserialize(new FileInputStream(path));
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T unserialize(InputStream in) throws Exception
	{
		ObjectInputStream ois = new ObjectInputStream(in);
		T theObj = (T) ois.readObject();
		ois.close();
		return theObj;
	}	
	
	@SuppressWarnings("unchecked")
	public static <T> T unserializeE(InputStream in) 
	{
		try { return (T)unserialize(in); }
		catch (Exception ex) { throw new RuntimeException(ex); } 
	}
	
	// Copied from
	// http://stackoverflow.com/questions/106770/standard-concise-way-to-copy-a-file-in-java
	public static void copyFile(File sourceFile, File destFile) throws IOException {
		if(!destFile.exists()) {
			destFile.createNewFile();
		}
		
		FileChannel source = null;
		FileChannel destination = null;
		
		try {
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			destination.transferFrom(source, 0, source.size());
		}
		finally {
			if(source != null) {
				source.close();
			}
			if(destination != null) {
				destination.close();
			}
		}
	}
	
        public static BufferedReader getReader(String filename, String encoding) throws IOException
        {
        	InputStream fileStream = new FileInputStream(filename);
        	return new BufferedReader(new InputStreamReader(fileStream, encoding));
        }        
       
        public static BufferedReader getReader(String filename) throws IOException
        {
        	return getReader(filename, BidLogEntry.BID_LOG_CHARSET);
        }                
        
        public static BufferedReader getGzipReader(String filename, String encoding) throws IOException
        {
        	InputStream fileStream = new FileInputStream(filename);
        	InputStream gzipStream = new GZIPInputStream(fileStream);
        	return new BufferedReader(new InputStreamReader(gzipStream, encoding));
        }
               
        public static BufferedReader getGzipReader(String filename) throws IOException
        {
        	return getGzipReader(filename,  BidLogEntry.BID_LOG_CHARSET);
        }

	
	
	public static BufferedWriter getWriter(String path) throws IOException
	{
		return new BufferedWriter(new FileWriter(path));	
	}
	
	public static BufferedWriter getGzipWriter(String path) throws IOException
	{
		return getGzipWriter(path, BidLogEntry.BID_LOG_CHARSET);
	}		
	
	public static BufferedWriter getGzipWriter(String path, String charsetName) throws IOException
	{
		GZIPOutputStream gzos = new GZIPOutputStream(new FileOutputStream(path));
		return new BufferedWriter(new OutputStreamWriter(gzos, charsetName));	
	}	
	
	public static LineReader bufRead2Line(final BufferedReader bread)
	{
		return new LineReader() {
			
			public String readLine() throws IOException { return bread.readLine(); }
			public void close() throws IOException { bread.close(); }
		};
		
	}
	
	
	public static File grabTempFile()
	{
		for(int i = 0; i < 1000; i++)
		{
			long a = Math.round((10000000) * Math.random());
			String temppath = Util.sprintf("/tmp/java_temp_%d.txt", a);
			File tfile = new File(temppath);

			if(!tfile.exists())
				{ return tfile; }
		}
		
		throw new RuntimeException("Could not find good temp file.");		
	}
	
	public static List<File> recFileCheck(File basedir, Pattern pathcheck, boolean checkfullpath)
	{
		Util.massert(basedir != null && basedir.exists() && basedir.isDirectory(),
			"Bad starting directory %s", basedir);
		
		List<File> flist = Util.vector();
		
		for(File kid : basedir.listFiles())
		{
			if(kid.isDirectory())
			{
				List<File> sublist = recFileCheck(kid, pathcheck, checkfullpath);
				flist.addAll(sublist);
				
			} else {
				
				String tocheck = (checkfullpath ? kid.getAbsolutePath() : kid.getName());
				
				if(pathcheck.matcher(tocheck).matches())
					{ flist.add(kid); }
			}
		}
		
		return flist;
	}
	
}
