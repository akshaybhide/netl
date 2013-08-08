package com.digilant.mobile;

import java.util.Calendar;
import java.util.GregorianCalendar;

import com.digilant.fastetl.FastUtil;
import com.digilant.fastetl.FastUtil.BlockCode;
import com.adnetik.shared.Util;

public class MobileUtil {
	public static String encloseInSingleQuotes(String input){
		StringBuffer sb = new StringBuffer();
		sb.append("'");
		sb.append(input);
		sb.append("'");
		return sb.toString();
	}
	public static String getDateKeyString(Calendar c, String delim){
		StringBuffer sb= new StringBuffer();
		String month = ((c.get(Calendar.MONTH)+ 1) < 10)?"0"+(c.get(Calendar.MONTH)+ 1):""+(c.get(Calendar.MONTH)+ 1);
		String day = (c.get(Calendar.DATE) < 10)?"0"+c.get(Calendar.DATE):""+c.get(Calendar.DATE);
		sb.append(c.get(Calendar.YEAR));
		sb.append(delim);
		sb.append(month);
		sb.append(delim);
		sb.append(day);
		return sb.toString();
	}
	public static String getLongDateCodeString(Calendar c){
		String date = getDateKeyString(c,"-");
		StringBuffer sb= new StringBuffer();
		sb.append(date);
		sb.append(" ");
		String hh = ""+c.get(Calendar.HOUR_OF_DAY);
		String mm = ""+c.get(Calendar.MINUTE);
		String ss = ""+c.get(Calendar.SECOND);
		sb.append(hh);
		sb.append(":");
		sb.append(mm);
		sb.append(":");
		sb.append(ss);
		return sb.toString();
	}
	public static int getPrevQuartet(){
		
		GregorianCalendar c = new GregorianCalendar();
		String today = getLongDateCodeString(c);
		
		BlockCode bcode = FastUtil.BlockCode.prevBlock(today);
		return bcode.getQuartet();
	}


}
