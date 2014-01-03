package com.cisco.bigdata.hadoop;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveJdbcDriver {
	
	public static void main(String[] args){
		try{
			long t1=System.currentTimeMillis();
		Class.forName("org.apache.hadoop.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "");  
	    Statement stmt = con.createStatement();  
	    String tableName = "errorlogs";  
	    //stmt.executeQuery("drop table " + tableName);  
	    //ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");  
	    // show tables  
	    String sql = "select logtime,classname,substr(errmsg,0,20) from " + tableName + " limit 3";  
	    System.out.println("Running: " + sql);  
	    ResultSet res = stmt.executeQuery(sql);  
	    while(res.next()) {  
	      System.out.println(res.getString(1)+"\t"+res.getString(2)+"\t"+res.getString(3));  
	    }
	    
	    String sql2 = "select count(logtime) from " + tableName ;  
	    System.out.println("Running: " + sql2);  
	    ResultSet res2 = stmt.executeQuery(sql2);  
	    if(res2.next()) {  
	      System.out.println(res2.getInt(1));  
	    }
	    long t2=System.currentTimeMillis();
	    System.out.println("above 2 sql spent "+(t2-t1)+"ms");
		}catch(ClassNotFoundException e){
			e.printStackTrace();
		}catch(SQLException ex){
			ex.printStackTrace();
		}
	}

}
