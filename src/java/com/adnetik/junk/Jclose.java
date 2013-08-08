
package com.adnetik.shared;

import java.util.*;

public class Jclose 
{

	public static <T> List<T> filter(Collection<T> tlist, F1<T> funcop)
	{
		List<T> sublist = Util.vector();
		
		for(T t : tlist)
		{ 
			Boolean b = Util.cast(funcop.f(t));	
			if(b)
				{ sublist.add(t); }
		}
		
		return sublist;
	}
	
	public static <R,S> List<R> map(Collection<S> srclist, F1<S> funcop)
	{
		List<R> retlist = Util.vector();
		
		for(S s : srclist)
		{ 
			R r = Util.cast(funcop.f(s));
			retlist.add(r);
		}
		
		return retlist;
		
	}
	
	public static <R, A, B> List<R> map(Map<A, B> srcmap, F2<A, B> doublef)
	{
		List<R> rlist = Util.vector();
		
		for(A key : srcmap.keySet())
		{
			R r = Util.cast(doublef.f(key, srcmap.get(key)));	
			rlist.add(r);
		}
		
		return rlist;
	}
	
	public static <A, B> B reduce(Collection<A> src, F2<A, B> redfunc, B init)
	{
		B curval = init;	
		
		for(A a : src)
		{
			curval = Util.cast(redfunc.f(a, curval));	
		}
		return curval;
	}
	
	
	public static <A, B> Map<A, B> filter(Map<A, B> srcmap, F2<A, B> funcop)
	{
		Map<A, B> resmap = Util.treemap();
		
		for(A a : srcmap.keySet())
		{
			Boolean accept = Util.cast(funcop.f(a, srcmap.get(a)));
			
			if(accept)
				{ resmap.put(a, srcmap.get(a)); }
		}

		return resmap;
	}	
	
	
	public interface F1<A>
	{
		public Object f(A a);	
		
	}
	
	public interface F2<A, B>
	{
		public Object f(A a, B b);	
	}


}
