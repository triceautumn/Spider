package injector;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import data_structure.url_data;

public class injector {
	public void inject(long interval) throws IOException {
		String currentPath=System.getProperty("user.dir");
		JobClient client = new JobClient();
		JobConf conf = new JobConf(injector.class);
		
		Path dbpath=new Path(currentPath+"/crawl/db");
		Path tmppath=new Path(currentPath+"/crawl/new");
		Path inject=new Path(currentPath+"/crawl/inject");
		Path input=new Path(currentPath+"/crawl/input");
		
		conf.setLong("interval", interval);
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		// TODO: specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(input))
			FileSystem.get(conf).mkdirs(input);
		
		if(FileSystem.get(conf).exists(inject));
			FileSystem.get(conf).delete(inject, true);
			
		FileInputFormat.addInputPath(conf, input);
		FileOutputFormat.setOutputPath(conf, inject);

		// TODO: specify a mapper
		conf.setMapperClass(injectorMap.class);

		// TODO: specify a reducer
		conf.setReducerClass(injectorReduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		conf=new JobConf(injector.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		// TODO: specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(dbpath))
			FileSystem.get(conf).mkdirs(dbpath);
		FileInputFormat.addInputPath(conf, new Path(currentPath+"/crawl/inject"));
		FileInputFormat.addInputPath(conf, dbpath);
		if(FileSystem.get(conf).exists(tmppath))
		  FileSystem.get(conf).delete(tmppath, true);
		FileOutputFormat.setOutputPath(conf, tmppath);

		// TODO: specify a mapper
		conf.setMapperClass(updateMap.class);

		// TODO: specify a reducer
		conf.setReducerClass(updateReduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
			if(FileSystem.get(conf).exists(tmppath)) {
				if(FileSystem.get(conf).exists(dbpath)) {
					FileSystem.get(conf).delete(dbpath, true);
					FileSystem.get(conf).rename(tmppath, dbpath);
				}
			}
			if(FileSystem.get(conf).exists(inject))
				FileSystem.get(conf).delete(inject, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		String currentPath=System.getProperty("user.dir");
		JobClient client = new JobClient();
		JobConf conf = new JobConf(injector.class);
		
		Path dbpath=new Path(currentPath+"/crawl/db");
		Path tmppath=new Path(currentPath+"/crawl/new");
		Path inject=new Path(currentPath+"/crawl/inject");
		Path input=new Path(currentPath+"/crawl/input");

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		// TODO: specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(input))
			FileSystem.get(conf).mkdirs(input);
		FileInputFormat.addInputPath(conf, input);
		if(FileSystem.get(conf).exists(inject));
			FileSystem.get(conf).delete(inject, true);
		FileOutputFormat.setOutputPath(conf, inject);

		// TODO: specify a mapper
		conf.setMapperClass(injectorMap.class);

		// TODO: specify a reducer
		conf.setReducerClass(injectorReduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		conf=new JobConf(injector.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);

		// TODO: specify input and output DIRECTORIES (not files)
		if(!FileSystem.get(conf).exists(dbpath))
			FileSystem.get(conf).mkdirs(dbpath);
		FileInputFormat.addInputPath(conf, new Path(currentPath+"/crawl/inject"));
		FileInputFormat.addInputPath(conf, dbpath);
		if(FileSystem.get(conf).exists(tmppath))
		  FileSystem.get(conf).delete(tmppath, true);
		FileOutputFormat.setOutputPath(conf, tmppath);

		// TODO: specify a mapper
		conf.setMapperClass(updateMap.class);

		// TODO: specify a reducer
		conf.setReducerClass(updateReduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
			if(FileSystem.get(conf).exists(tmppath)) {
				if(FileSystem.get(conf).exists(dbpath)) {
					FileSystem.get(conf).delete(dbpath, true);
					FileSystem.get(conf).rename(tmppath, dbpath);
				}
			}
			if(FileSystem.get(conf).exists(inject))
				FileSystem.get(conf).delete(inject, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
