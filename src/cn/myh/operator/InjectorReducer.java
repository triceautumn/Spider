package cn.myh.operator;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;

public class InjectorReducer extends MapReduceBase 
	implements Reducer<Text, UrlData, Text, UrlData> {

	public void reduce(Text _key, Iterator<UrlData> values,
			OutputCollector<Text, UrlData> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		Text key = (Text) _key;
		UrlData newdata=new UrlData();
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			UrlData value = values.next();
			if(value.getStatus()==UrlData.STATUS_INJECTED) {
				newdata.set(value);
				newdata.setStatus(UrlData.STATUS_DB_UNFETCHED);
			}
		}
		output.collect(key, newdata);
	}
}
