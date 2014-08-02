package cn.myh.operator;

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

import cn.myh.bean.UrlData;

public class Injector {
	public void inject(long interval) throws IOException {
		String currentPath=System.getProperty("user.dir");
		JobClient client = new JobClient();
		JobConf conf = new JobConf(Injector.class);
		
		Path dbpath=new Path(currentPath+"/crawl/db");
		Path tmppath=new Path(currentPath+"/crawl/new");
		Path inject=new Path(currentPath+"/crawl/inject");
		Path input=new Path(currentPath+"/crawl/input");
		
		conf.setLong("interval", interval);
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(UrlData.class);
		
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
		conf.setMapperClass(InjectorMapper.class);

		// TODO: specify a reducer
		conf.setReducerClass(InjectorReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		conf=new JobConf(Injector.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(UrlData.class);
		
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
		conf.setMapperClass(UpdateMapper.class);

		// TODO: specify a reducer
		conf.setReducerClass(UpdateReducer.class);

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
		JobConf conf = new JobConf(Injector.class);
		
		Path dbpath=new Path(currentPath+"/crawl/db");
		Path tmppath=new Path(currentPath+"/crawl/new");
		Path inject=new Path(currentPath+"/crawl/inject");
		Path input=new Path(currentPath+"/crawl/input");

		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(UrlData.class);
		
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
		conf.setMapperClass(InjectorMapper.class);

		// TODO: specify a reducer
		conf.setReducerClass(InjectorReducer.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		conf=new JobConf(Injector.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(UrlData.class);
		
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
		conf.setMapperClass(UpdateMapper.class);

		// TODO: specify a reducer
		conf.setReducerClass(UpdateReducer.class);

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
