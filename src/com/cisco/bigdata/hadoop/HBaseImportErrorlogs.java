package com.cisco.bigdata.hadoop;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class HBaseImportErrorlogs {
	private static HTable table;
	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	public static class HBaseImportErrorlogsMapper 
	extends Mapper<Object, Text, NullWritable, NullWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			String[] fields=value.toString().split("\t");
			if(fields.length>0){
				String logtime=fields[0];
				long stamp=0;
				try{
				Date d=sdf.parse(logtime);
				stamp=d.getTime();
				}catch(ParseException ex){
					
				}
				String classname=fields[1];
				String errmsg=fields[2];
				byte[] rowKey=Bytes.toBytes(classname);
				
				Put p=new Put(rowKey);
				p.add(Bytes.toBytes("data"), Bytes.toBytes("logtime"), Bytes.toBytes(logtime));
				p.add(Bytes.toBytes("data"), Bytes.toBytes("stamp"), Bytes.toBytes(stamp+""));
				p.add(Bytes.toBytes("data"), Bytes.toBytes("errmsg"), Bytes.toBytes(errmsg));
				table.put(p);
				
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			table.close();
		}
		public void setup(Context context) throws IOException, InterruptedException {
			table=new HTable(context.getConfiguration(),Bytes.toBytes("test"));
		}
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 1) {
	      System.err.println("Usage: HBaseImportErrorlogs <in>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "HBase Import Error logs");
	    job.setJarByClass(HBaseImportErrorlogs.class);
	    job.setMapperClass(HBaseImportErrorlogsMapper.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    job.setOutputFormatClass(NullOutputFormat.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
