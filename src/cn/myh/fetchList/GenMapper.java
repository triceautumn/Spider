package cn.myh.fetchList;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;

public class GenMapper extends MapReduceBase implements 
	Mapper<Text, UrlData, Text, UrlData> {

	@Override
	public void map(Text arg0, UrlData arg1,
			OutputCollector<Text, UrlData> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		//long defInterval=1000*3600*24;
		
		if(arg1.getStatus()==UrlData.STATUS_DB_UNFETCHED) {
			arg1.setlastFetchTime(System.currentTimeMillis());
			//arg1.setFetchInterval(defInterval);
			arg2.collect(arg0, arg1);
		}
		else if(arg1.getStatus()==UrlData.STATUS_DB_FETCHED) {
			long updateTime=arg1.getlastFetchTime()+arg1.getFetchInterval();
			if(updateTime<=System.currentTimeMillis()) {
				arg1.setStatus(UrlData.STATUS_DB_UNFETCHED);
				arg1.setlastFetchTime(System.currentTimeMillis());
				//arg1.setFetchInterval(defInterval);
				arg2.collect(arg0, arg1);
			}
		}
	}
}
