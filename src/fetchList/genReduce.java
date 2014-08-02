package fetchList;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;

public class genReduce extends MapReduceBase implements 
	Reducer<Text, url_data, Text, url_data> {

	@Override
	public void reduce(Text arg0, Iterator<url_data> arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		url_data data=new url_data();
		long recent=0;
		while(arg1.hasNext()) {
			url_data tmp=new url_data();
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
