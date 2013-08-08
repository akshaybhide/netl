package com.digilant.fastetl;

import java.util.*;
import java.io.*;

import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.Util;
import com.digilant.fastetl.FastUtil.MyLogType;

import java.util.*;

public class InterestManager
{
	String interestdir;
	String lidpath;
	String pidpath;
	Set<Integer> lineset = Util.treeset();
	Set<Integer> pixset = Util.treeset();
	FileManager _fileman;
	public InterestManager(FileManager f){
		_fileman = f;
		interestdir = _fileman.getInterestPath();
		lidpath = interestdir + "/lineitemlist2";
		pidpath = interestdir + "/pixellist2";
	}
	void runPythonScript(){
		Runtime rt = Runtime.getRuntime();
		String[] cmd = {interestdir + "/retrieve_lineitem_pixel_from_uidb.py",  lidpath,  pidpath};
		try {
			Process pr = rt.exec(cmd);
			pr.waitFor();
			
			Thread.sleep(5000);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*try {
			Util.syscall("/usr/bin/python \\ rretrieve_lineitem_pixel_from_uidb.py lineitemfile pixelfile");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/ catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	void loadCurrent()
	{
		runPythonScript();
		setPixelInterest();
		setLineItemInterest();
		// Do whatever is necessary to get the current interest set	
		
	}
	public Set<Integer> getPixelInterest(){
		return pixset;
	}
	
	public void setPixelInterest()
	{
		
		
		/*for(int i = 1000; i < 2000; i++)
			{ pixset.add(i); }
		*/
		try {
			Scanner scan = new Scanner(new File(pidpath));
			while(scan.hasNext()){
				pixset.add(scan.nextInt());
			}
			scan.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//pixset.add
				
		
	}
	public Set<Integer> getLineItemInterest()
	{
		return lineset;
	}	
	public void setLineItemInterest()
	{
		//Set<Integer> lineset = Util.treeset();
/*		lineset.add(1887948628);
		lineset.add(1887956173);
		lineset.add(1887960392);
		lineset.add(1887961554);
		lineset.add(1887962162);
		lineset.add(1887962541);
		lineset.add(1887963125);
		lineset.add(1887964590);
		lineset.add(1887961920);
		lineset.add(1887964011);*/
		try {
			Scanner scan = new Scanner(new File(lidpath));
			while(scan.hasNext()){
				lineset.add(scan.nextInt());
			}
			scan.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	public boolean PixelExists(Integer pid){
		return pixset.contains(pid);
	}
	
	public boolean LineItemExists(Integer lid){
		return lineset.contains(lid);
	}
	static public void main(String[] args){
		/*InterestManager im = new InterestManager();
		im.loadCurrent();
		Util.pf("lineitem size : %d \n", im.getLineItemInterest().size());
		for(Integer i : im.getLineItemInterest())
			Util.pf("lineitem  : %d \n", i);
		for(Integer i : im.getPixelInterest())
			Util.pf("pixel  : %d \n", i);
			*/
	}
	
}
