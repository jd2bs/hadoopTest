package com.cisco.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class ErrorLogFormat {

	public static class TokenizerMapper 
    extends Mapper<Object, Text, NullWritable, Text>{
		private Text word = new Text();
		private Text tempMsg=new Text();
		private Text logtime=new Text();
		private Text classname=new Text();
		private NullWritable k= NullWritable.get();
		private BooleanWritable writeFlag=new BooleanWritable(false);
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	//String logTime="",classname="";
	    	String tempString=value.toString();
          if(tempString.indexOf("-[ERROR] -")>0){
        	//first write last error block  
        	if(logtime.getLength()>0){
        		word.set(logtime.toString()+"\t"+classname.toString()+"\t"+tempMsg.toString());
        		context.write(k, word);
        		logtime.clear();
        		classname.clear();
        		tempMsg.clear();
        		
        	}
            int p1=tempString.indexOf("-[ERROR] -");
            String s1=tempString.substring(0, p1);
            String s2=tempString.substring(p1+10, tempString.length());
            //System.out.println("s1="+s1);
            //System.out.println("s2="+s2);
            //System.out.println(tempArr[3]);
            //System.out.println(tempArr[4]);
            //System.out.println(tempArr[5]);
            
            tempMsg.set(s2);
            String[] tempArr2=s1.split(" "); 
            logtime.set(tempArr2[0]+" "+tempArr2[1]);
            classname.set(tempArr2[2]);
            }else{
            String errmsg=tempMsg.toString()+"|||"+tempString.replaceAll("\t", "    ");
            tempMsg.set(errmsg);
            }
            
            
            //if(!"".equals(logTime)){
            //word.set(logTime+"\t"+classname+"\t"+tempMsg.toString());
            
            
            //
            //}
            
	    }
	    
	    public void cleanup(Context context) throws IOException, InterruptedException {
	    	//At last: we write the last error block  
        	if(logtime.getLength()>0){
        		word.set(logtime.toString()+"\t"+classname.toString()+"\t"+tempMsg.toString());
        		context.write(k, word);
        		logtime.clear();
        		classname.clear();
        		tempMsg.clear();
        		
        	}
	    }
	}
	
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 2) {
	      System.err.println("Usage: ErrorLogFormat <in> <out>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "Error log format");
	    job.setJarByClass(ErrorLogFormat.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setNumReduceTasks(0);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }
}
