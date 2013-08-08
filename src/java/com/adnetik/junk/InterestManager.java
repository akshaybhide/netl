package com.adnetik.fastetl;

import java.util.*;
import java.io.*;

import com.adnetik.fastetl.FastUtil.MyLogType;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.*;

import java.util.*;

public class InterestManager
{
	String interestdir = FastUtil.getInterestPath();
	String lidpath = interestdir + "/lineitemlist2";
	String pidpath = interestdir + "/pixellist2";
	Set<Integer> lineset = Util.treeset();
	Set<Integer> pixset = Util.treeset();
	
	void runPythonScript(){
		Runtime rt = Runtime.getRuntime();
		String[] cmd = {interestdir + "/retrieve_lineitem_pixel_from_uidb.py",  lidpath,  pidpath};
		try {
			Process pr = rt.exec(cmd);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		/*try {
			Util.syscall("/usr/bin/python \\ rretrieve_lineitem_pixel_from_uidb.py lineitemfile pixelfile");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		
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
	
	static public void main(String[] args){
		InterestManager im = new InterestManager();
		im.loadCurrent();
		Util.pf("lineitem size : %d \n", im.getLineItemInterest().size());
		for(Integer i : im.getLineItemInterest())
			Util.pf("lineitem  : %d \n", i);
		for(Integer i : im.getPixelInterest())
			Util.pf("pixel  : %d \n", i);
			
	}
	
}
