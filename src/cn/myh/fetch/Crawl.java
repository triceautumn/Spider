package cn.myh.fetch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;



public class Crawl extends MapReduceBase implements 
	Mapper<Text, UrlData, Text, Text>, Reducer<Text, Text, Text, Text> {

	@Override
	public void map(Text arg0, UrlData arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		String content=HerfMatch.getContent(arg0.toString());
		//arg1.setDescription(content);
		//arg1.setStatus(url_data.STATUS_DB_FETCHED);
		arg2.collect(arg0, new Text(content));
	}

	@Override
	public void reduce(Text arg0, Iterator<Text> arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3) throws IOException {
		// TODO Auto-generated method stub
		while(arg1.hasNext()) {
			arg2.collect(arg0, arg1.next());
		}
	}
}
