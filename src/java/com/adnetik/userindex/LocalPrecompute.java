
package com.adnetik.userindex;

import java.io.*;
import java.util.*;
import java.net.*;

import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.filecache.*;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.CompressionCodec;
import com.hadoop.compression.lzo.LzopCodec;

import com.adnetik.shared.*;
import com.adnetik.userindex.*;

import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;
import com.adnetik.userindex.UserIndexUtil.*;

import com.adnetik.data_management.*;

public class LocalPrecompute
{	
	ScanRequest _scanReq;
	
	String _blockEnd;
	
	FileSystem _fSystem;
	
	Random _randCheck = new Random();
	
	private long _userCount = 0;
	
	private static String LOCAL_PREC_DIR = "/local/fellowship/userindex/localprec";
	
	public static void main(String[] args) throws Exception
	{
		ScanRequest screq = ScanRequest.buildFromListCode("pixel_8599");
		LocalPrecompute lprec = new LocalPrecompute(screq, "2013-04-07");
		
		// lprec.prepData();
		lprec.doPreCompute();
	}
	
	LocalPrecompute(ScanRequest scanreq, String be) throws IOException
	{
		UserIndexUtil.assertValidBlockEnd(be);	
		
		_blockEnd = be;
		_scanReq = scanreq;
		
		_fSystem = FileSystem.get(new Configuration());
	}
	
	void prepData() throws IOException
	{
		grab2Local();
		Util.unixsort(getLocTempPath(), "");
	}
	
	private void grab2Local() throws IOException
	{
		BufferedWriter bwrite = FileUtils.getWriter(getLocTempPath());
		List<String> daylist = UserIndexUtil.getCanonicalDayList(_blockEnd);	
		
		for(String oneday : daylist)
		{
			String dbslicepath = Util.sprintf("/userindex/dbslice/%s/%s.%s", oneday, _scanReq.getListCode(), UserIndexUtil.SLICE_SUFF);
			
			BufferedReader bread = HadoopUtil.getGzipReader(_fSystem, dbslicepath);
			
			for(String oneline = bread.readLine(); oneline != null; oneline = bread.readLine())
			{
				bwrite.write(oneline);
				bwrite.write("\n");
			}
			
			bread.close();
			
			Util.pf("Finished spooling file %s\n", dbslicepath);
		}
		
		bwrite.close();		
	}
	
	void doPreCompute() throws IOException
	{
		checkInitFeatMan();		
		
		BufferedReader bread = FileUtils.getReader(getLocTempPath());
		SortedFileMap sfm = new SortedFileMap(bread, 50000, 0, true);
		
		BufferedWriter bwrite = FileUtils.getWriter(getLocOutputPath());

		double startup = Util.curtime();		
		while(sfm.hasNext())
		{
			// Skip if the user count is greater than the max useful number
			if(_userCount >= UserIndexUtil.MAX_USERS_PRECOMP)
				{ break; }		
			
			
			Map.Entry<String, List<String>> nextpair = sfm.pollFirstEntry();
							
			String[] listcode_wtp = nextpair.getKey().toString().split(Util.DUMB_SEP);
			WtpId userwtp = (listcode_wtp.length < 2 ? null : WtpId.getOrNull(listcode_wtp[1]));

			// Util.pf("Found user %s has %d callouts\n", 
			//	userwtp, nextpair.getValue().size());
			
			// Check that key is correctly formatted.
			if(userwtp == null)
			{ 
				Util.pf("Bad pair key %s\n", nextpair.getKey());
				continue;
			}			
			
			// Maybe this assertion is taking a long time.... ?
			// All data for a single listcode gets sent to the same reducer
			Util.massert(_scanReq.getListCode().equals(listcode_wtp[0]),
				"Mixed data in reducer, found scanrequest %s and listcode %s",
				_scanReq.getListCode(), listcode_wtp[0]);
			
			UserPack curpack = new UserPack();
			curpack.userId = listcode_wtp[1];			
			
			ScoreUserJob.popUPackFrmRdcrData(curpack, nextpair.getValue().iterator());

			Set<Integer> pixset = getPixSet(curpack);
			if(!pixset.isEmpty())
				{ Util.pf("Pixel set is %s\n", pixset); }

			// Util.pf("\nFound %d callouts for user %s", curpack.getData().size(), curpack.userId);
			List<String> keyval = UserIndexPrecompute.getKeyValList(curpack, _scanReq, _randCheck);

			// Write the output
			{
				bwrite.write(curpack.userId);
				bwrite.write("\t");
				bwrite.write(Util.join(keyval, "\t"));
				bwrite.write("\n");
			}
			
			_userCount++;
			
			if((_userCount % 20) == 0)
			{
				double userpersec = (_userCount)/((Util.curtime()-startup)/1000);
				Util.pf("Finished with user %d, ID %s, avg %.03f user/sec\n", 
					_userCount, userwtp, userpersec);
			}
		}
		
		// 
		bwrite.close();
		
		// There may be issues with this close call, because SFM closes the reader for you
		sfm.close();
	}
	
	Set<Integer> getPixSet(UserPack upack)
	{
		Set<Integer> pixset = Util.treeset();
		
		for(PixelLogEntry ple : upack.getPixList())
		{
			pixset.add(ple.getIntField(LogField.pixel_id));	
		}
		
		return pixset;
	}
	
	void checkInitFeatMan() throws IOException
	{
		if(!UFeatManager.isSingReady())
		{
			String blockend = TimeUtil.cal2DayCode(UserIndexUtil.getCanonicalEndDay());
			UFeatManager.initSing(_blockEnd);
		}		
	}

	String getLocTempPath()
	{
		return Util.sprintf("%s/dbslice__%s_%s.txt", LOCAL_PREC_DIR, _scanReq.getListCode(), _blockEnd);
	}
	
	String getLocOutputPath()
	{
		// TODO: figure out the right output
		return Util.sprintf("%s/prec__%s_%s.prec", LOCAL_PREC_DIR, _scanReq.getListCode(), _blockEnd);
	}
}
