package com.cisco.webex.bigdata.kaifang;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class HBaseImportKaiFangInfo {
//rowKey:[cardID][timestamp]?total 33 fields
	private static HTable table;
	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public static class HBaseImportKaiFangInfoMapper 
	extends Mapper<Object, Text, NullWritable, NullWritable>{
		public void map(Object key, Text value, Context context
                ) throws IOException, InterruptedException {
			String[] fields=value.toString().split(",");
			if(fields.length>31&&(fields[4].length()==18)){
				String kaifangtime=fields[31];
				long stamp=0;
				try{
				Date d=sdf.parse(kaifangtime);
				stamp=d.getTime();
				}catch(ParseException ex){
					
				}
				String username=fields[0];
				String cardType=fields[3];
				String cardID=fields[4];
				byte[] rowKey=makeKaiFangRowKey(cardID,stamp);
				
				Put p=new Put(rowKey);
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.CID, Bytes.toBytes(cardID));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.UN, Bytes.toBytes(username));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.KFT, Bytes.toBytes(kaifangtime));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.STP, Bytes.toBytes(stamp+""));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.CAT, Bytes.toBytes(cardType));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.GEN, Bytes.toBytes(fields[5]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.BD, Bytes.toBytes(fields[6]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.ADD, Bytes.toBytes(fields[7]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.DT, Bytes.toBytes(fields[9]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.CT, Bytes.toBytes(fields[11]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.FLO, Bytes.toBytes(fields[12]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.ROOM, Bytes.toBytes(fields[13]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.MOB, Bytes.toBytes(fields[19]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.TEL, Bytes.toBytes(fields[20]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.NA, Bytes.toBytes(fields[23]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.EML, Bytes.toBytes(fields[22]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.EDU, Bytes.toBytes(fields[25]));
				p.add(HBaseKFConstants.DAT, HBaseKFConstants.CNTID, Bytes.toBytes(fields[32]));
				table.put(p);
				table.flushCommits();
				
			}
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException {
			table.close();
		}
		public void setup(Context context) throws IOException, InterruptedException {
			table=new HTable(context.getConfiguration(),Bytes.toBytes("kaifang"));
			table.setAutoFlush(true);
		}
	}
	public static byte[] makeKaiFangRowKey(String cardID, long kaifangStamp){
		byte[] row=new byte[HBaseKFConstants.CardID_LENGTH+Bytes.SIZEOF_LONG];
		Bytes.putBytes(row, 0, Bytes.toBytes(cardID), 0, HBaseKFConstants.CardID_LENGTH);
		long reverseOrder=Long.MAX_VALUE-kaifangStamp;
		Bytes.putLong(row, HBaseKFConstants.CardID_LENGTH, reverseOrder);
		return row;
	}
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 1) {
	      System.err.println("Usage: HBaseImportKaiFangInfo <in>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "HBase Import KaiFang info");
	    job.setJarByClass(HBaseImportKaiFangInfo.class);
	    job.setMapperClass(HBaseImportKaiFangInfoMapper.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(NullWritable.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    job.setOutputFormatClass(NullOutputFormat.class);
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
