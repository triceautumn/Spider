package fetch;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;

public class view extends MapReduceBase implements
	Mapper<Text, url_data, Text, Text>, Reducer<Text, Text, Text, Text>{

	@Override
	public void map(Text arg0, url_data arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg2.collect(arg0, new Text("1"));
	}

	@Override
	public void reduce(Text arg0, Iterator<Text> arg1,
			OutputCollector<Text, Text> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		int sum=0;
		while(arg1.hasNext()) {
			sum+=Integer.parseInt(arg1.next().toString());
		}
		String s=""+sum;
		arg2.collect(new Text(s), arg0);
	}

}
