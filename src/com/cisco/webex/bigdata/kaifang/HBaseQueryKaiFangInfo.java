package com.cisco.webex.bigdata.kaifang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseQueryKaiFangInfo {
	private static final int CardID_LENGTH=18;
	public static List<KFUserBean> getKaiFangInfoByCardID(HTable table,String cardID,long maxStamp,int maxCount) throws IOException{
		byte[] startrowkey=makeKaiFangRowKey(cardID,maxStamp);
		
		List<KFUserBean> list=new ArrayList<KFUserBean>();
		Scan scan=new Scan(startrowkey);
		//scan.setFilter(new SingleColumnValueFilter(HBaseKFConstants.DAT,HBaseKFConstants.CID, CompareOp.EQUAL, Bytes.toBytes(cardID)));
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.CID);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.UN);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.KFT);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.STP);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.CAT);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.GEN);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.BD);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.ADD);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.DT);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.CT);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.FLO);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.ROOM);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.MOB);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.TEL);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.NA);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.EML);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.EDU);
		scan.addColumn(HBaseKFConstants.DAT, HBaseKFConstants.CNTID);
		ResultScanner scanner=table.getScanner(scan);
		
		Result res=null;
		int count=0;
		try{
			while((res=scanner.next())!=null&&(count++<maxCount)){
				//byte[] row=res.getRow();
				KFUserBean kfub=new KFUserBean();
				SetValuesForKFUB(kfub,res);
				list.add(kfub);
			}
		}finally{
			scanner.close();
		}
		
		return list;
	}
	public static byte[] makeKaiFangRowKey(String cardID, long kaifangStamp){
		byte[] row=new byte[CardID_LENGTH+Bytes.SIZEOF_LONG];
		Bytes.putBytes(row, 0, Bytes.toBytes(cardID), 0, CardID_LENGTH);
		long reverseOrder=Long.MAX_VALUE-kaifangStamp;
		Bytes.putLong(row, CardID_LENGTH, reverseOrder);
		return row;
	}
	public static String getValue(Result res ,byte [] cf,byte [] qua ){
		byte[] value=res.getValue(cf, qua);
		return value==null?"":Bytes.toString(value);
	}

	public static void SetValuesForKFUB(KFUserBean kfub,Result res){
	kfub.setCardID(	getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.CID));
	kfub.setUsername(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.UN));
	kfub.setKaifangtime(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.KFT));
	kfub.setStamp(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.STP));
	kfub.setCardType(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.CAT));
	kfub.setGender(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.GEN));
	kfub.setBirthday(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.BD));
	kfub.setAddress(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.ADD));
	kfub.setDirty(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.DT));
	kfub.setCountry(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.CT));
	kfub.setFloor(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.FLO));
	kfub.setRoom(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.ROOM));
	kfub.setMobile(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.MOB));
	kfub.setTel(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.TEL));
	kfub.setNation(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.NA));
	kfub.setEmail(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.EML));
	kfub.setEducation(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.EDU));
	kfub.setCountid(getValue(res,HBaseKFConstants.DAT, HBaseKFConstants.CNTID));
	}
	
	public static void main(String[] args) throws IOException{
		HTable table=new HTable(HBaseConfiguration.create(),Bytes.toBytes("kaifang"));
		long t1=System.currentTimeMillis();
		List list=getKaiFangInfoByCardID(table,"420711197509192410",Long.MAX_VALUE,10);
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
		
		
	}
}
