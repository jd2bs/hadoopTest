package com.cisco.webex.bigdata.kaifang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.util.Bytes;

public class TableRowCounter {
	
	public static void main(String[] args) throws Throwable{
		long t1=System.currentTimeMillis();
		Configuration conf=new Configuration();
		conf.setLong("hbase.rpc.timeout", 600000);
		conf.setLong("hbase.client.scanner.caching", 1000);
		
		Configuration confs=HBaseConfiguration.create(conf);
		AggregationClient ac=new AggregationClient(confs);
		
		Scan scan=new Scan();
		scan.addFamily(Bytes.toBytes("data"));
		long rowcount=ac.rowCount(Bytes.toBytes("kaifang"), null, scan);
		System.out.println("row count="+rowcount);
		long t2=System.currentTimeMillis();
		System.out.println("spend "+(t2-t1)+" ms");
	}

}
