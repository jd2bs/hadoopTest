package com.cisco.bigdata.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class HBaseQuery {
	
	public static Map<String,String> getErrorLogByClassname(HTable table,String cls) throws IOException{
		Get get=new Get(Bytes.toBytes(cls));
		get.addFamily(Bytes.toBytes("data"));
		
		Result res=table.get(get);
		if(res==null){
			return null;
		}
		
		Map<String,String> resultMap=new HashMap<String,String>();
		resultMap.put("logtime", getValue(res,Bytes.toBytes("data"),Bytes.toBytes("logtime")));
		resultMap.put("stamp", getValue(res,Bytes.toBytes("data"),Bytes.toBytes("stamp")));
		resultMap.put("errmsg", getValue(res,Bytes.toBytes("data"),Bytes.toBytes("errmsg")));
		return resultMap;
	}
	public static String getValue(Result res ,byte [] cf,byte [] qua ){
		byte[] value=res.getValue(cf, qua);
		return value==null?"":Bytes.toString(value);
	}
	
	public static void getCounter(String[] args1,long t2) throws IOException, ClassNotFoundException, InterruptedException{
		Job job=RowCounter.createSubmittableJob(HBaseConfiguration.create(), args1);
		if(job.waitForCompletion(true)){
			long t3=System.currentTimeMillis();
			
			System.out.println("total rows="+job.getCounters().countCounters());
			//System.out.println("total rows2="+(RowCounter.RowCounterMapper)job.getMapperClass().);
			System.out.println("spend "+(t3-t2)+" ms");
		}
		//return 0;
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		HTable table=new HTable(HBaseConfiguration.create(),Bytes.toBytes("test"));
		long t1=System.currentTimeMillis();
		Map<String ,String> map=getErrorLogByClassname(table,"[remoting]");
		System.out.println(map.get("logtime"));
		System.out.println(map.get("errmsg"));
		System.out.println(map.get("stamp"));
		long t2=System.currentTimeMillis();
		System.out.println("spend "+(t2-t1)+" ms");
		String[] ar={"test"};
		getCounter(ar,t2);
		
	}
}
