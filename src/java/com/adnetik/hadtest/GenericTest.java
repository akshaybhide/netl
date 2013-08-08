/**
 * 
 */
package com.adnetik.hadtest;

import java.io.*;
import java.util.*;
import java.sql.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

import com.adnetik.shared.*;
import com.adnetik.bm_etl.*;
import com.adnetik.shared.BidLogEntry.*;


public class GenericTest
{

	public static void main(String[] args) throws Exception
	{

	}
	
	public static <A> List<A> getMyList()
	{
		List<A> mylist = Util.vector();
		
		for(int i = 0; i < 1000; i++)
		{
			String s = "danburfoot";
			A a = Util.cast(s);
			mylist.add(a);		
		}
		return mylist;
	}
}
