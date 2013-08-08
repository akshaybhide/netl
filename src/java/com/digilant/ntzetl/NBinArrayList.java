package com.digilant.ntzetl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class NBinArrayList<E> extends ArrayList<E> {

	/**
	 * 
	 */
	 
	private static final long serialVersionUID = 1L;
	
	public int MaxSize;
	public int next;
	public NBinArrayList(int n){
		super();
		MaxSize = n;
		
	}
	@Override
	public Object clone() {
		// TODO Auto-generated method stub
		NBinArrayList<E> tmp = new NBinArrayList<E>(MaxSize);
		return tmp;
		
		
	}
	@Override
	public boolean add(E e) {
		// TODO Auto-generated method stub
		if(next==MaxSize){
			next=0;
		}
		super.add(next++, e);
		if(super.size()==MaxSize+1)
			super.remove(MaxSize);
		return true;
		
	}
	@Override
	public void add(int index, E element) {
		// TODO Auto-generated method stub
		if(index<MaxSize){
			super.add(index, element);
		}
		else
			{	if(next==MaxSize) next=0;
				super.add(next++,element);
			}
		if(super.size()==MaxSize+1)
			super.remove(MaxSize);
	}
	@Override
	public E remove(int index) {
		// TODO Auto-generated method stub
		System.out.println("remove does nothing for this Array");
		return null;
	}
	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		System.out.println("remove does nothing for this Array");
		return true;
	}
	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		super.addAll(next, c);
		if(next+c.size()-1 > MaxSize)
			next=0;
		else
			next+=c.size();
		while(super.size()>MaxSize){
			super.remove(super.size()-1);
		}
		
		return true;
	}
	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		// TODO Auto-generated method stub
		System.out.println("addAll does nothing for this Array");
		return true;
	}
	@Override
	protected void removeRange(int fromIndex, int toIndex) {
		// TODO Auto-generated method stub
		System.out.println("remove does nothing for this Array");
	}
	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		System.out.println("remove does nothing for this Array");
		return true;
	}
	
	public static void main(String[] args){
		NBinArrayList<Integer> nb_arr = new NBinArrayList<Integer>(5);
		nb_arr.add(1);
		nb_arr.add(2);
		nb_arr.add(3);
		nb_arr.add(4);
		nb_arr.add(5);
		System.out.println(nb_arr);
		nb_arr.add(6);
		nb_arr.add(7);
		nb_arr.add(8);
		System.out.println(nb_arr);
		nb_arr.add(5,10);
		System.out.println(nb_arr);
		nb_arr.add(11);
		System.out.println(nb_arr);
		nb_arr.add(12);
		System.out.println(nb_arr);
		ArrayList tmp = new ArrayList(Arrays.asList(1,2));
		nb_arr.addAll(tmp);
		System.out.println(nb_arr);
		nb_arr.add(22);
		System.out.println(nb_arr);
		
	}
}
