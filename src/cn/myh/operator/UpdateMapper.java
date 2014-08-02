package cn.myh.operator;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import cn.myh.bean.UrlData;

public class UpdateMapper extends MapReduceBase 
	implements Mapper<Text, UrlData, Text, UrlData> {

	@Override
	public void map(Text arg0, UrlData arg1,
			OutputCollector<Text, UrlData> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg2.collect(arg0, arg1);
	}
}
