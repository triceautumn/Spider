package cn.myh.fetch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;
/***
 * 类似一个UrlData过滤过程，把状态改变一下
 * @author hello
 *
 */
public class Update implements Mapper<Text, UrlData, Text, UrlData>, 
	Reducer<Text, UrlData, Text, UrlData> {

	@Override
	public void map(Text arg0, UrlData arg1,
			OutputCollector<Text, UrlData> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		UrlData tmp=new UrlData();
		if(arg1.getStatus()==UrlData.STATUS_DB_READYTOFETCH) {
			tmp.set(arg1);
			tmp.setStatus(UrlData.STATUS_DB_FETCHED);
		}
		arg2.collect(arg0, arg1); 
	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void reduce(Text arg0, Iterator<UrlData> arg1,
			OutputCollector<Text, UrlData> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		UrlData tmp=new UrlData();
		while(arg1.hasNext()) {
			tmp.set(arg1.next());
		}
		tmp.setlastFetchTime(System.currentTimeMillis());
		tmp.setStatus(UrlData.STATUS_DB_FETCHED);
		arg2.collect(arg0, tmp);
	}
}
