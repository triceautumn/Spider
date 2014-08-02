package fetch;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import data_structure.url_data;

public class fetching implements Mapper<Text, url_data, Text, url_data>
	, Reducer<Text, url_data, Text, url_data>{
		
		ArrayList<String> urls=new ArrayList<String>();
		
	@Override
	public void map(Text arg0, url_data arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		if(arg1.getStatus()==url_data.STATUS_DB_UNFETCHED) {
			arg1.setStatus(url_data.STATUS_DB_READYTOFETCH);
			arg2.collect(arg0, arg1);
			urls=HerfMatch.FindURL(arg0.toString());
			if(urls==null)
				return;
			for(int i=0;i<urls.size();i++) {
				url_data tmp=new url_data();
				String url=urls.get(i);
				tmp.set(arg1);
				tmp.setStatus(url_data.STATUS_DB_UNFETCHED);
				tmp.setlastFetchTime(System.currentTimeMillis());
				arg2.collect(new Text(url), tmp);
			}
		}else {
			arg2.collect(arg0, arg1);
		}
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
		url_data data=new url_data();
		while(arg1.hasNext()) {
			data.set(arg1.next());
		}
		arg2.collect(arg0, data);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 * @throws IOException 
	 */
	public void fetch(int d) throws IOException {
		int depth=d;
		JobClient client=new JobClient();
		JobConf conf=new JobConf(fetching.class);
		/***
		 * 第一个map-reduce程序，存储结果首先放在在tmp，最后tmp修改名称为db，将DB中的url的data的标志修改为STATUS_DB_FETCHED
		 */
		conf.setJobName("updating db");
		String current=System.getProperty("user.dir")+"/crawl";
		Path fetchlist=new Path(current+"/fetch_list");
		Path tmp=new Path(current+"/tmp");
		Path db=new Path(current+"/db");
		Path stored=new Path(current+"/stored_web/"+new Date(System.currentTimeMillis()));
		//设置reduce的输出格式
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		//设置任务的输入格式
		conf.setInputFormat(SequenceFileInputFormat.class);
		//设置reduce输出格式
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		if(!FileSystem.get(conf).exists(fetchlist))
			FileSystem.get(conf).mkdirs(fetchlist);
		if(!FileSystem.get(conf).exists(db))
			FileSystem.get(conf).mkdirs(db);
	   
		
		/** 
         * InputFormat描述map-reduce中对job的输入定义 
         * setInputPaths():为map-reduce job设置路径数组作为输入列表 
         * setInputPath()：为map-reduce job设置路径数组作为输出列表 
         */  
		//设置输入文件的路径，可以使一个文件，一个路径，一个通配符。可以被调用多次添加多个路径 
		FileInputFormat.addInputPath(conf, db);
		FileInputFormat.addInputPath(conf, fetchlist);
		
		if(FileSystem.get(conf).exists(tmp))
			FileSystem.get(conf).delete(tmp, true);
		
		//设置输出文件的路径，在job运行前此路径不应该存在
		FileOutputFormat.setOutputPath(conf, tmp);
		
		//设置Mapper，默认为IdentityMapper 
		conf.setMapperClass(update.class);
		conf.setReducerClass(update.class);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
			if(FileSystem.get(conf).exists(db))
				FileSystem.get(conf).delete(db, true);
			FileSystem.get(conf).rename(tmp, db);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
//*************************************************************************		

	/***
	 * 第二个map-reduce程序，爬取深度为depth的从fetchlist中读取，最后仍填写在fetchlist中
	 */
		for(int i=0;i<depth;i++) {
			conf=new JobConf(fetching.class);
			conf.setJobName("updating fetch list");
			
			//设置Reducer的输出的key-value对的格式 
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(url_data.class);
			
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf, fetchlist);
			if(FileSystem.get(conf).exists(tmp))
				FileSystem.get(conf).delete(tmp, true);
			
			FileOutputFormat.setOutputPath(conf, tmp);
			
			conf.setMapperClass(fetching.class);
			conf.setReducerClass(fetching.class);
			
			client.setConf(conf);
			try {
				JobClient.runJob(conf);
				if(FileSystem.get(conf).exists(fetchlist))
					FileSystem.get(conf).delete(fetchlist, true);
				FileSystem.get(conf).rename(tmp, fetchlist);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		conf=new JobConf(fetching.class);
		conf.setJobName("crawling...");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		
		FileInputFormat.setInputPaths(conf, fetchlist);
		FileOutputFormat.setOutputPath(conf, stored);
		
		conf.setMapperClass(view.class);
		conf.setReducerClass(view.class);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
			if(FileSystem.get(conf).exists(fetchlist))
				FileSystem.get(conf).delete(fetchlist, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		int depth=3;
		JobClient client=new JobClient();
		JobConf conf=new JobConf(fetching.class);
		conf.setJobName("updating db");
		
		String current=System.getProperty("user.dir")+"/crawl";
		Path fetchlist=new Path(current+"/fetch_list");
		Path tmp=new Path(current+"/tmp");
		Path db=new Path(current+"/db");
		Path stored=new Path(current+"/stored_web/"+new Date(System.currentTimeMillis()));
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		if(!FileSystem.get(conf).exists(fetchlist))
			FileSystem.get(conf).mkdirs(fetchlist);
		if(!FileSystem.get(conf).exists(db))
			FileSystem.get(conf).mkdirs(db);
		
		FileInputFormat.addInputPath(conf, db);
		FileInputFormat.addInputPath(conf, fetchlist);
		if(FileSystem.get(conf).exists(tmp))
			FileSystem.get(conf).delete(tmp, true);
		FileOutputFormat.setOutputPath(conf, tmp);
		
		conf.setMapperClass(update.class);
		conf.setReducerClass(update.class);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
			if(FileSystem.get(conf).exists(db))
				FileSystem.get(conf).delete(db, true);
			FileSystem.get(conf).rename(tmp, db);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for(int i=0;i<depth;i++) {
			conf=new JobConf(fetching.class);
			conf.setJobName("updating fetch list");
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(url_data.class);
			
			conf.setInputFormat(SequenceFileInputFormat.class);
			conf.setOutputFormat(SequenceFileOutputFormat.class);
			
			FileInputFormat.setInputPaths(conf, fetchlist);
			if(FileSystem.get(conf).exists(tmp))
				FileSystem.get(conf).delete(tmp, true);
			FileOutputFormat.setOutputPath(conf, tmp);
			
			conf.setMapperClass(fetching.class);
			conf.setReducerClass(fetching.class);
			
			client.setConf(conf);
			try {
				JobClient.runJob(conf);
				if(FileSystem.get(conf).exists(fetchlist))
					FileSystem.get(conf).delete(fetchlist, true);
				FileSystem.get(conf).rename(tmp, fetchlist);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		conf=new JobConf(fetching.class);
		conf.setJobName("crawling...");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, fetchlist);
		FileOutputFormat.setOutputPath(conf, stored);
		
		conf.setMapperClass(view.class);
		conf.setReducerClass(view.class);
		
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
			if(FileSystem.get(conf).exists(fetchlist))
				FileSystem.get(conf).delete(fetchlist, true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
