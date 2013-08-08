
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

public class MemoryTest
{		
	public enum MyOption { use_a, use_b };
	
	
	public static void main(String[] args)
	{
		MyOption myopt = MyOption.valueOf(args[0]); // Error if invalid CL arg 
		
		// Random myrand = new Random();
		List mylist = new LinkedList();		
		Integer a = 675;
		String  b = "675";
		
		/*
		while(true)
		{						
			mylist.add((myopt == MyOption.use_a ? a : b));
			
			if((mylist.size() % 1000) == 0)
			{
				// System.out.printf("Just added toadd=%d\n", toadd);
				System.out.printf("List size is now %d\n", mylist.size());	
			}
		}
		*/
	}
}
