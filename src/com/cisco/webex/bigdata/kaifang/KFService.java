package com.cisco.webex.bigdata.kaifang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class KFService {

	public List<KFUserBean> getKFUserInfoByCardID(String cardID){
		List list=null;
		HTable table=null;
		try{
		table=new HTable(HBaseConfiguration.create(),Bytes.toBytes("kaifang"));
		long t1=System.currentTimeMillis();
		list=HBaseQueryKaiFangInfo.getKaiFangInfoByCardID(table,cardID,Long.MAX_VALUE,3);
		Iterator it=list.iterator();
		while(it.hasNext()){
			KFUserBean kfub=(KFUserBean)it.next();
			System.out.println("username="+kfub.getUsername());
			System.out.println("kaifangtime="+kfub.getKaifangtime());
			System.out.println("address="+kfub.getAddress());
			System.out.println("gender="+kfub.getGender());
			System.out.println("mobile="+kfub.getMobile());
			System.out.println("***********************");
		}
		long t2=System.currentTimeMillis();
		System.out.println("spend "+(t2-t1)+" ms");
		
		}catch(IOException ioe){
			ioe.printStackTrace();
		}finally{
			if(table!=null){
			   try{	
				   table.close();
			   }catch(IOException ioef){
					ioef.printStackTrace();
				}
			}
		}
		return list;
	}
	
	public List<KFUserBean> getKFUserInfoByUserName(String username) throws SolrServerException{
		List<KFUserBean> list=new ArrayList<KFUserBean>();
		SolrServer server=null;
		server=new HttpSolrServer("http://192.168.1.102:8080/solr/collection1");
		String queryStr="username:\""+username+"\"";
		SolrQuery q=new SolrQuery();
		q.setQuery(queryStr);
		//q.setStart(1);
		//q.setRows(10);
		QueryResponse resp=server.query(q);
		SolrDocumentList doclist=resp.getResults();
		Iterator it=doclist.iterator();
		while(it.hasNext()){
			SolrDocument doc=(SolrDocument)it.next();
			String cardID=(String)doc.getFieldValue("cardID");
			list.addAll(this.getKFUserInfoByCardID(cardID));
		}
		return list;
	}
}
