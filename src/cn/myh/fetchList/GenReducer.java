package cn.myh.fetchList;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;

public class GenReducer extends MapReduceBase implements 
	Reducer<Text, UrlData, Text, UrlData> {

	@Override
	public void reduce(Text arg0, Iterator<UrlData> arg1,
			OutputCollector<Text, UrlData> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		UrlData data=new UrlData();
		long recent=0;
		while(arg1.hasNext()) {
			UrlData tmp=new UrlData();
			tmp.set(arg1.next());
			if(recent<tmp.getlastFetchTime()) {
				recent=tmp.getlastFetchTime();
				data.set(tmp);
			}
		}
		if(recent!=0)
			arg2.collect(arg0, data);
	}
}
