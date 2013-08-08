
package com.adnetik.adhoc;

import java.io.*;
import java.util.*;

import com.adnetik.shared.*;
import com.adnetik.shared.Util.*;
import com.adnetik.shared.BidLogEntry.*;

import com.adnetik.bm_etl.*;
import com.adnetik.bm_etl.BmUtil.*;

import com.adnetik.analytics.*;

import java.math.BigInteger;

public class LSCQuery
{	
	private Map<BigInteger, BigInteger> _agcy2Acct = Util.treemap();
	private Map<BigInteger, BigInteger> _camp2Agcy = Util.treemap();
	private Map<BigInteger, BigInteger> _camp2Clnt = Util.treemap();
	private Map<BigInteger, BigInteger> _clnt2Acct = Util.treemap();
	
	private Map<BigInteger, String> _campNameMap = Util.treemap();
	
	public static void main(String[] args)
	{
		Util.pf("Hello from LSCQuery\n");	
		
		
		LSCQuery lscq = new LSCQuery();
		lscq.initFromDb();
		
		long[] accttarg = new long[] { 739, 1852 };
		
		for(long oneacct : accttarg)
		{
			Util.pf("Found following campaigns for account %d: %s\n", 
				oneacct, lscq.camps4Acct(oneacct));			
		}
	}

	
	void initFromDb()
	{
		{
			String sql = "SELECT id, account_id FROM adnetik.agency";
			DbUtil.popMapFromQuery(_agcy2Acct, sql, new DatabaseBridge(DbTarget.external));
		} {
			String sql = "SELECT id, agency_id FROM adnetik.campaign WHERE agency_id IS NOT NULL";
			DbUtil.popMapFromQuery(_camp2Agcy, sql, new DatabaseBridge(DbTarget.external));			
		} {
			String sql = "SELECT id, client_id FROM adnetik.campaign WHERE client_id IS NOT NULL";
			DbUtil.popMapFromQuery(_camp2Clnt, sql, new DatabaseBridge(DbTarget.external));			
		} {
			String sql = "SELECT id, account_id FROM adnetik.client WHERE account_id IS NOT NULL";
			DbUtil.popMapFromQuery(_clnt2Acct, sql, new DatabaseBridge(DbTarget.external));			
		} {
			String sql = "SELECT id, name FROM adnetik.campaign";
			DbUtil.popMapFromQuery(_campNameMap, sql, new DatabaseBridge(DbTarget.external));			
		}
		
		Util.pf("Found %d elements in agcy2acct map\n", _agcy2Acct.size());
		Util.pf("Found %d elements in camp2agcy map\n", _camp2Agcy.size());
		Util.pf("Found %d elements in camp2agcy map\n", _camp2Clnt.size());
		Util.pf("Found %d elements in camp2agcy map\n", _clnt2Acct.size());
	}
	
	public String getCampaignName(Number campid)
	{
		return _campNameMap.get(new BigInteger(campid.toString()));
		
	}
	
	Set<BigInteger> camps4Acct(long acctid)
	{
		Set<BigInteger> campset = Util.treeset();
		
		{
			Set<BigInteger> clntset = setLookup(_clnt2Acct, acctid);
			campset.addAll(setLookup(_camp2Clnt, clntset));
		}
		
		{
			Set<BigInteger> agcyset = setLookup(_agcy2Acct, acctid);
			campset.addAll(setLookup(_camp2Agcy, agcyset));
		}		
		
		return campset;
		
	}
	
	
	Set<BigInteger> camps4Agcy(long agcyid)
	{
		return setLookup(_camp2Agcy, agcyid);
	}
	
	Set<BigInteger> agencies4Acct(long acctid)
	{
		return setLookup(_agcy2Acct, acctid);
	}
	
	Set<BigInteger> clients4Acct(long acctid)
	{
		return setLookup(_clnt2Acct, acctid);
	}	
	
	private static Set<BigInteger> setLookup(Map<BigInteger, BigInteger> relmap, long targid)
	{
		Set<BigInteger> idset = Util.treeset();
		idset.add(new BigInteger(targid+""));
		return setLookup(relmap, idset);
		
	}
	
	private static Set<BigInteger> setLookup(Map<BigInteger, BigInteger> relmap, Set<BigInteger> targset)
	{
		Set<BigInteger> relset = Util.treeset();
		
		for(Map.Entry<BigInteger, BigInteger> one_ent : relmap.entrySet())
		{
			if(targset.contains(one_ent.getValue()))
				{ relset.add(one_ent.getKey()); }
		}
		
		return relset;		
	}		
	
	
	
	
}
