package fetch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;
/***
 * 类似一个url_data过滤过程，把状态改变一下
 * @author hello
 *
 */
public class update implements Mapper<Text, url_data, Text, url_data>, 
	Reducer<Text, url_data, Text, url_data> {

	@Override
	public void map(Text arg0, url_data arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		url_data tmp=new url_data();
		if(arg1.getStatus()==url_data.STATUS_DB_READYTOFETCH) {
			tmp.set(arg1);
			tmp.setStatus(url_data.STATUS_DB_FETCHED);
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
	public void reduce(Text arg0, Iterator<url_data> arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		url_data tmp=new url_data();
		while(arg1.hasNext()) {
			tmp.set(arg1.next());
		}
		tmp.setlastFetchTime(System.currentTimeMillis());
		tmp.setStatus(url_data.STATUS_DB_FETCHED);
		arg2.collect(arg0, tmp);
	}
}
