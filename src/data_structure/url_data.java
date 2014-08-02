package data_structure;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.io.WritableComparable;

public class url_data implements WritableComparable<url_data>, Cloneable{
	private byte status;
	private long lastFetchTime;
	private long fetchInterval;
	private String description;
	
	/** Page was not fetched yet. */
	public static final byte STATUS_DB_UNFETCHED      = 0x01;
	/** Page was successfully fetched. */
	public static final byte STATUS_DB_FETCHED        = 0x02;
	/** Page no longer exists. */
	public static final byte STATUS_DB_GONE           = 0x03;
	/** Page temporarily redirects to other page. */
	public static final byte STATUS_DB_REDIR_TEMP     = 0x04;
	/** Page permanently redirects to other page. */
	public static final byte STATUS_DB_REDIR_PERM     = 0x05;
	/** Page was successfully fetched and found not modified. */
	public static final byte STATUS_DB_READYTOFETCH   = 0x06;
	  
	/** Maximum value of DB-related status. */
  public static final byte STATUS_DB_MAX            = 0x1f;
	  
	/** Fetching was successful. */
	public static final byte STATUS_FETCH_SUCCESS     = 0x21;
	/** Fetching unsuccessful, needs to be retried (transient errors). */
	public static final byte STATUS_FETCH_RETRY       = 0x22;
	/** Fetching temporarily redirected to other page. */
	public static final byte STATUS_FETCH_REDIR_TEMP  = 0x23;
	/** Fetching permanently redirected to other page. */
	public static final byte STATUS_FETCH_REDIR_PERM  = 0x24;
	/** Fetching unsuccessful - page is gone. */
	public static final byte STATUS_FETCH_GONE        = 0x25;
	/** Fetching successful - page is not modified. */
	public static final byte STATUS_FETCH_NOTMODIFIED = 0x26;
	  
	/** Maximum value of fetch-related status. */
	public static final byte STATUS_FETCH_MAX         = 0x3f;
	  
	/** Page signature. */
	public static final byte STATUS_SIGNATURE         = 0x41;
	/** Page was newly injected. */
	public static final byte STATUS_INJECTED          = 0x42;
	/** Page discovered through a link. */
	public static final byte STATUS_LINKED            = 0x43;
	  
	  
	public static final HashMap<Byte, String> statNames = new HashMap<Byte, String>();
	static {
	  statNames.put(STATUS_DB_UNFETCHED, "db_unfetched");
	  statNames.put(STATUS_DB_FETCHED, "db_fetched");
	  statNames.put(STATUS_DB_GONE, "db_gone");
	  statNames.put(STATUS_DB_REDIR_TEMP, "db_redir_temp");
	  statNames.put(STATUS_DB_REDIR_PERM, "db_redir_perm");
	  statNames.put(STATUS_DB_READYTOFETCH, "db_ready_to_fetch");
	  statNames.put(STATUS_SIGNATURE, "signature");
	  statNames.put(STATUS_INJECTED, "injected");
	  statNames.put(STATUS_LINKED, "linked");
	  statNames.put(STATUS_FETCH_SUCCESS, "fetch_success");
	  statNames.put(STATUS_FETCH_RETRY, "fetch_retry");
	  statNames.put(STATUS_FETCH_REDIR_TEMP, "fetch_redir_temp");
	  statNames.put(STATUS_FETCH_REDIR_PERM, "fetch_redir_perm");
	  statNames.put(STATUS_FETCH_GONE, "fetch_gone");
	  statNames.put(STATUS_FETCH_NOTMODIFIED, "fetch_notmodified");
	}
	
	public url_data() {
		this((byte) 0);
	}
	
	public url_data(byte status) {
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
	
	
	public void set(url_data ud) {
		this.status=ud.status;
		this.description=ud.description;
		this.lastFetchTime=ud.lastFetchTime;
		this.fetchInterval=ud.fetchInterval;
	}

	public static url_data read(DataInput input) throws IOException {
		url_data result=new url_data();
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
	public int compareTo(url_data right) {
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
