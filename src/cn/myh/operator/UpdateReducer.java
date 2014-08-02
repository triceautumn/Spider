package cn.myh.operator;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;

public class UpdateReducer extends MapReduceBase 
	implements Reducer<Text, UrlData, Text, UrlData> {

	@Override
	public void reduce(Text arg0, Iterator<UrlData> arg1,
			OutputCollector<Text, UrlData> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		UrlData old=new UrlData();
		UrlData newd=new UrlData();
		while(arg1.hasNext()) {
			UrlData ud=arg1.next();
			if(ud.getStatus()==UrlData.STATUS_DB_FETCHED) {
				old.set(ud);
				break;
			}else {
				newd.set(ud);
				//break;
			}
		}
		if(old.getStatus()!=0)
			arg2.collect(arg0, old);
		else {
			newd.setStatus(UrlData.STATUS_DB_UNFETCHED);
			arg2.collect(arg0, newd);
		}
	}
}
