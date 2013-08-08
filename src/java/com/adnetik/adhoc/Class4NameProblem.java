
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

public class Class4NameProblem
{			
	public static void main(String[] args) throws Exception
	{
		Class4NameInterface A = (Class4NameInterface) Class.forName("com.adnetik.adhoc.Class4NameBar").newInstance();
		System.out.printf("Result of A is %d\n", A.gimmeNumber());
		
		Class4NameInterface B = (Class4NameInterface) Class.forName("com.adnetik.adhoc.Class4NameProblem$Class4NameFoo").newInstance();		
		System.out.printf("Result of B is %d\n", B.gimmeNumber());
	}
	
	public interface Class4NameInterface
	{
		public abstract int gimmeNumber();	
	}
	
	public class Class4NameFoo implements Class4NameInterface
	{
		public int gimmeNumber()
		{
			return 20;	
		}
	} 
}
