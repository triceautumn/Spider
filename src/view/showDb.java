package view;

import java.io.IOException;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import data_structure.url_data;

public class showDb extends MapReduceBase implements 
	Mapper<Text, url_data, Text, url_data>{

	@Override
	public void map(Text arg0, url_data arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		arg2.collect(arg0, arg1);
	}
	
	public void show() throws IOException {
		JobClient client=new JobClient();
		JobConf conf =new JobConf(showDb.class);
		
		String current=System.getProperty("user.dir")+"/crawl";
		Path db=new Path(current+"/db");
		Path view=new Path(current+"/dbviewer");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, db);
		if(FileSystem.get(conf).exists(view))
			FileSystem.get(conf).delete(view, true);
		FileOutputFormat.setOutputPath(conf, view);
		
		conf.setMapperClass(showDb.class);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String [] args) throws IOException {
		JobClient client=new JobClient();
		JobConf conf =new JobConf(showDb.class);
		
		String current=System.getProperty("user.dir")+"/crawl";
		Path db=new Path(current+"/db");
		Path view=new Path(current+"/dbviewer");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, db);
		if(FileSystem.get(conf).exists(view))
			FileSystem.get(conf).delete(view, true);
		FileOutputFormat.setOutputPath(conf, view);
		
		conf.setMapperClass(showDb.class);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
