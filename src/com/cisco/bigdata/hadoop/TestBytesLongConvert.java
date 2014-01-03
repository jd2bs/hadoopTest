package com.cisco.bigdata.hadoop;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public class TestBytesLongConvert {
	
	public static void main(String[] args){
		String a="2013-09-12 23:23:35";
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		long stamp=0;
		try{
		Date d=sdf.parse(a);
		stamp=d.getTime();
		}catch(ParseException ex){
			
		}
		
		System.out.println("stamp="+stamp);
		byte[] b=Bytes.toBytes(stamp+"");
		
		long s1=Bytes.toLong(b);
		String s2=Bytes.toString(b);

		System.out.println("s1="+s1);

		System.out.println("s2="+s2);
	}

}
