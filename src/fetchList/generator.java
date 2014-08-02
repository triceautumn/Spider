package fetchList;

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

import data_structure.url_data;

public class generator {
	public void generate() throws IOException {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(fetchList.generator.class);

		String currentPath=System.getProperty("user.dir");
		Path in=new Path(currentPath+"/crawl/db");
		Path out=new Path(currentPath+"/crawl/fetch_list");
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);

		// TODO: specify input and output DIRECTORIES (not files)
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		if(!FileSystem.get(conf).exists(in))
			FileSystem.get(conf).mkdirs(in);
		
		FileInputFormat.setInputPaths(conf, in);
		if(FileSystem.get(conf).exists(out))
			FileSystem.get(conf).delete(out, true);
		FileOutputFormat.setOutputPath(conf, out);

		// TODO: specify a mapper
		conf.setMapperClass(genMap.class);

		// TODO: specify a reducer
		conf.setReducerClass(genReduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(fetchList.generator.class);

		String currentPath=System.getProperty("user.dir");
		Path in=new Path(currentPath+"/crawl/db");
		Path out=new Path(currentPath+"/crawl/fetch_list");
		// TODO: specify output types
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(url_data.class);

		// TODO: specify input and output DIRECTORIES (not files)
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		
		if(!FileSystem.get(conf).exists(in))
			FileSystem.get(conf).mkdirs(in);
		
		FileInputFormat.setInputPaths(conf, in);
		if(FileSystem.get(conf).exists(out))
			FileSystem.get(conf).delete(out, true);
		FileOutputFormat.setOutputPath(conf, out);

		// TODO: specify a mapper
		conf.setMapperClass(genMap.class);

		// TODO: specify a reducer
		conf.setReducerClass(genReduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
