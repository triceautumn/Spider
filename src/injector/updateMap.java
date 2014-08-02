package injector;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;

public class updateMap extends MapReduceBase 
	implements Mapper<Text, url_data, Text, url_data> {

	@Override
	public void map(Text arg0, url_data arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg2.collect(arg0, arg1);
	}
}
