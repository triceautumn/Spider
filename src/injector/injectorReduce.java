package injector;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;

public class injectorReduce extends MapReduceBase 
	implements Reducer<Text, url_data, Text, url_data> {

	public void reduce(Text _key, Iterator<url_data> values,
			OutputCollector<Text, url_data> output, Reporter reporter) throws IOException {
		// replace KeyType with the real type of your key
		Text key = (Text) _key;
		url_data newdata=new url_data();
		while (values.hasNext()) {
			// replace ValueType with the real type of your value
			url_data value = values.next();
			if(value.getStatus()==url_data.STATUS_INJECTED) {
				newdata.set(value);
				newdata.setStatus(url_data.STATUS_DB_UNFETCHED);
			}
		}
		output.collect(key, newdata);
	}
}
