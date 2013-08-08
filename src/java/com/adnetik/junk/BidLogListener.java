
package com.adnetik.userindex;

import java.util.*;
import java.io.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;

import com.adnetik.userindex.UserIndexUtil.*;


public interface BidLogListener
{
	
	public abstract void initialize();
	
	public abstract void inspect(BidLogEntry ble);
	
	
}
