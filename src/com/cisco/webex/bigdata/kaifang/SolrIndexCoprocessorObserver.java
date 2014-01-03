package com.cisco.webex.bigdata.kaifang;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;

public class SolrIndexCoprocessorObserver extends BaseRegionObserver {
	//public static SolrServer server=new HttpSolrServer("http://192.168.1.102:8080/solr/collection1");
	public SolrIndexCoprocessorObserver(){
		
	}
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,final Put put,final WALEdit edit,final boolean writeToWAL) throws IOException{
		
		
		byte[] rowkey=put.getRow();
		String rowkeystr=new String(rowkey,"UTF-8");
		
		List<KeyValue> kv=put.get(HBaseKFConstants.DAT, HBaseKFConstants.UN);
		String username=new String(kv.get(0).getValue(),"UTF-8");
		
		List<KeyValue> kv2=put.get(HBaseKFConstants.DAT, HBaseKFConstants.MOB);
		String mobile=new String(kv2.get(0).getValue(),"UTF-8");
		
		List<KeyValue> kv3=put.get(HBaseKFConstants.DAT, HBaseKFConstants.EML);
		String email=new String(kv3.get(0).getValue(),"UTF-8");
		List<KeyValue> kv4=put.get(HBaseKFConstants.DAT, HBaseKFConstants.CID);
		String cardID=new String(kv4.get(0).getValue(),"UTF-8");
		
		SolrInputDocument doc=new SolrInputDocument();
		doc.addField("id", cardID);
		doc.addField("cardID", cardID);
		doc.addField("rowkey",rowkeystr);
		doc.addField("username", username);
		doc.addField("email", email);
		doc.addField("mobile", mobile);
		try {
			SolrServer server=new HttpSolrServer("http://192.168.1.102:8080/solr/collection1");
			server.add(doc);
			server.commit();
		} catch (SolrServerException e1) {
			// TODO Auto-generated catch block
			System.out.println("solo server error="+e1.getMessage());
			e1.printStackTrace();
		}
		
		
		
	}
	
	public void start(final ObserverContext<RegionCoprocessorEnvironment> e){
		
		 
	}

	public void stop(final ObserverContext<RegionCoprocessorEnvironment> e) throws SolrServerException, IOException{
		//server.commit();
	}
}
 