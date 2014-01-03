package com.cisco.webex.bigdata.kaifang;

import org.apache.hadoop.hbase.util.Bytes;

public class HBaseKFConstants {

	public static final int CardID_LENGTH=18;
	public static final byte[] CID=Bytes.toBytes("cardID");
	public static final byte[] UN=Bytes.toBytes("username");
	public static final byte[] KFT=Bytes.toBytes("kaifangtime");
	public static final byte[] STP=Bytes.toBytes("stamp");
	public static final byte[] CAT=Bytes.toBytes("cardType");
	public static final byte[] GEN=Bytes.toBytes("gender");
	public static final byte[] BD=Bytes.toBytes("birthday");
	public static final byte[] ADD=Bytes.toBytes("address");
	public static final byte[] DT=Bytes.toBytes("dirty");
	public static final byte[] CT=Bytes.toBytes("country");
	public static final byte[] FLO=Bytes.toBytes("floor");
	public static final byte[] ROOM=Bytes.toBytes("room");
	public static final byte[] MOB=Bytes.toBytes("mobile");
	public static final byte[] TEL=Bytes.toBytes("tel");
	public static final byte[] NA=Bytes.toBytes("nation");
	public static final byte[] EML=Bytes.toBytes("email");
	public static final byte[] EDU=Bytes.toBytes("education");
	public static final byte[] CNTID=Bytes.toBytes("countid");
	public static final byte[] DAT=Bytes.toBytes("data");
}
