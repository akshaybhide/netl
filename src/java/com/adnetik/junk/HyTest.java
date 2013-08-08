
package com.adnetik.bm_etl;

import java.util.*;
import com.adnetik.shared.*;

import org.apache.hadoop.mapred.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;

public class HyTest
{

	public static void main(String[] args)
	{
		System.out.printf("Hello, world");

		List<String> flines = HadoopUtil.readFileLinesE("/home/burfoot/topcities/TopCityQuery/part-00000");

		Util.pf("Read %d lines", flines.size());
	}
}
