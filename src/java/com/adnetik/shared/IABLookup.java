
package com.adnetik.shared;

import java.util.*;
import java.io.*;
import java.net.*;
import java.sql.*;

import com.adnetik.shared.Util.*;

// Do not include Hadoop imports in this file


public class IABLookup
{
	private static final String IAB_LOOKUP_MAP = "/com/adnetik/resources/IAB_exc2iab.tsv";
	private static final String IAB_SEGMENT_NAMES = "/com/adnetik/resources/IAB_segnames.tsv";
	
	
	private static IABLookup _SING;
	
	private SortedMap<Pair<ExcName, Integer>, Set<Integer>> _lookupMap = Util.treemap();
	
	private TreeMap<Integer, String> _nameMap = Util.treemap();
	
	public static synchronized IABLookup getSing()
	{
		if(_SING == null)
		{
			_SING = new IABLookup();	
			_SING.init();
		}
		
		return _SING;
	}
	
	
	public Map<Integer, String> getNameMap()
	{
		return Collections.unmodifiableMap(_nameMap);	
	}
	
	private void init()
	{
		{
			int lcount = 0;
			InputStream resource = (IABLookup.class.getResourceAsStream(IAB_SEGMENT_NAMES));
			
			Scanner sc = new Scanner(resource, "UTF-8");
			
			sc.nextLine(); // skip header row
			
			while(sc.hasNextLine())
			{
				String[] segname_id = sc.nextLine().split("\t");
				_nameMap.put(Integer.valueOf(segname_id[1]), segname_id[0]);
			}
			sc.close();	
			
			Util.pf("Found %d IAB segments, first is %s, last is %s\n", 
				_nameMap.size(), _nameMap.firstEntry(), _nameMap.lastEntry());
			
		}
		
		
		{
			int lcount = 0;
			InputStream resource = (IABLookup.class.getResourceAsStream(IAB_LOOKUP_MAP));
			
			Scanner sc = new Scanner(resource, "UTF-8");
			
			sc.nextLine(); // skip header row
			
			while(sc.hasNextLine())
			{
				String[] name_excid_iabid = sc.nextLine().split("\t");
				
				ExcName excname = ExcName.valueOf(name_excid_iabid[0]);
				Integer exc_id = Integer.valueOf(name_excid_iabid[1]);
				Integer iab_id = Integer.valueOf(name_excid_iabid[2]);
				
				Pair<ExcName, Integer> pairkey = Pair.build(excname, exc_id);
				Util.setdefault(_lookupMap, pairkey, new TreeSet<Integer>());
				_lookupMap.get(pairkey).add(iab_id);
				lcount++;
			}
			sc.close();	
			
			Util.pf("Read %d pairs, %d total lines\n", _lookupMap.size(), lcount);
		}
		
		
	}
	
	public Set<Integer> excId2IabId(ExcName excname, Integer excid)
	{
		return _lookupMap.get(Pair.build(excname, excid));
	}
	
	private void uploadNameMap()
	{
		try { 
			Connection conn = createConnection();	
			PreparedStatement pstmt = conn.prepareStatement("INSERT INTO IAB_info.seginfo VALUES ( ? , ? )");
			
			for(Map.Entry<Integer, String> myentry : _nameMap.entrySet())
			{
				pstmt.setInt(1, myentry.getKey());
				pstmt.setString(2, myentry.getValue());
				pstmt.executeUpdate();
			}
			
			
		} catch (SQLException sqlex) {
			
			sqlex.printStackTrace();	
		}
		
		
	}
	
	private Connection createConnection() throws SQLException
	{
		return DbUtil.getDbConnection("thorin.adnetik.com", "IAB_info");
	}
	
	public static void main(String[] args) throws Exception
	{
		getSing();
		
		getSing().uploadNameMap();
	}
}

