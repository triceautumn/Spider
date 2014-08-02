package cn.myh.operator;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;

public class InjectorMapper extends MapReduceBase 
	implements Mapper<WritableComparable, Text, Text, UrlData> {
		long interval;

	public void map(WritableComparable key, Text values,
			OutputCollector<Text, UrlData> output, Reporter reporter) throws IOException {
		String url=values.toString();
		if(url!=null) {
			UrlData data=new UrlData(UrlData.STATUS_INJECTED);
			data.setFetchInterval(this.interval);
			values.set(url);
			output.collect(values, data);
		}
	}
	
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
		interval=arg0.getLong("interval", 1000*3600*24);
	}
}
