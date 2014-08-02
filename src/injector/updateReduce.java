package injector;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;

public class updateReduce extends MapReduceBase 
	implements Reducer<Text, url_data, Text, url_data> {

	@Override
	public void reduce(Text arg0, Iterator<url_data> arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		url_data old=new url_data();
		url_data newd=new url_data();
		while(arg1.hasNext()) {
			url_data ud=arg1.next();
			if(ud.getStatus()==url_data.STATUS_DB_FETCHED) {
				old.set(ud);
				break;
			}else {
				newd.set(ud);
				//break;
			}
		}
		if(old.getStatus()!=0)
			arg2.collect(arg0, old);
		else {
			newd.setStatus(url_data.STATUS_DB_UNFETCHED);
			arg2.collect(arg0, newd);
		}
	}
}
