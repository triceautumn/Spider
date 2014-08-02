package cn.myh.bean;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.WritableComparable;

public class UrlData implements WritableComparable<UrlData>, Cloneable{
	private byte status;
	private long lastFetchTime;
	private long fetchInterval;
	private String description;
	
	/** 未爬取 */
	public static final byte STATUS_DB_UNFETCHED      = 0x01;
	/** 成功爬取 */
	public static final byte STATUS_DB_FETCHED        = 0x02;
	
	/** 页面被成功爬取 */
	public static final byte STATUS_DB_READYTOFETCH   = 0x06;

	/** 页面新注入 */
	public static final byte STATUS_INJECTED          = 0x42;
	
	public static final HashMap<Byte, String> statNames = new HashMap<Byte, String>();
	static {
	  statNames.put(STATUS_DB_UNFETCHED, "db_unfetched");
	  statNames.put(STATUS_DB_FETCHED, "db_fetched");
	  statNames.put(STATUS_DB_READYTOFETCH, "db_ready_to_fetch");
	  statNames.put(STATUS_INJECTED, "injected");
	}
	
	public UrlData() {
		this((byte) 0);
	}
	
	public UrlData(byte status) {
		this.status=status;
		this.fetchInterval=Long.MAX_VALUE;
		this.lastFetchTime=0;
		this.description=null;
	}
	
	public void setStatus(byte s) {
		this.status=s;
	}
	
	public byte getStatus() {
		return this.status;
	}
	
	public void setlastFetchTime(long times) {
		this.lastFetchTime=times;
	}
	
	public long getlastFetchTime() {
		return this.lastFetchTime;
	}
	
	public long getFetchInterval() {
		return this.fetchInterval;
	}
	
	public void setFetchInterval(long interval) {
		this.fetchInterval=interval;
	}
	
	public void setDescription(String s) {
		this.description=s;
	}
	
	public String getDescription() {
		return this.description;
	}
	
	
	public void set(UrlData ud) {
		this.status=ud.status;
		this.description=ud.description;
		this.lastFetchTime=ud.lastFetchTime;
		this.fetchInterval=ud.fetchInterval;
	}

	public static UrlData read(DataInput input) throws IOException {
		UrlData result=new UrlData();
		result.readFields(input);
		return result;
	}
	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		this.status=input.readByte();
		this.lastFetchTime=input.readLong();
		this.fetchInterval=input.readLong();
		//this.description=input.readLine();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		output.writeByte(this.status);
		output.writeLong(this.lastFetchTime);
		output.writeLong(this.fetchInterval);
		//output.writeChars(this.description+"\n");
	}

	@Override
	public int compareTo(UrlData right) {
		// TODO Auto-generated method stub
		if(this.status!=right.status)
			return right.status-this.status;
		if(this.lastFetchTime!=right.lastFetchTime)
			return (right.lastFetchTime-this.lastFetchTime)>0?1:-1;
		if(this.fetchInterval!=right.fetchInterval)
			return (right.fetchInterval-this.fetchInterval)>0?-1:1;
		return 0;
	}
	
	public String toString() {
		StringBuilder buf = new StringBuilder();
	  buf.append("Status: " + this.status + " (" + this.statNames.get(this.status) + ")\n");
	  buf.append("Last fetch time: " + new Date(this.lastFetchTime) + "\n");
	  buf.append("Next fetch time: " + new Date(this.lastFetchTime+this.fetchInterval) + "\n");
	  buf.append("Description: \n" + this.description + "\n");
	  return buf.toString();
	}
}
