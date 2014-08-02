package fetchList;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import data_structure.url_data;

public class genMap extends MapReduceBase implements 
	Mapper<Text, url_data, Text, url_data> {

	@Override
	public void map(Text arg0, url_data arg1,
			OutputCollector<Text, url_data> arg2, Reporter arg3)
			throws IOException {
		// TODO Auto-generated method stub
		
		//long defInterval=1000*3600*24;
		
		if(arg1.getStatus()==url_data.STATUS_DB_UNFETCHED) {
			arg1.setlastFetchTime(System.currentTimeMillis());
			//arg1.setFetchInterval(defInterval);
			arg2.collect(arg0, arg1);
		}
		else if(arg1.getStatus()==url_data.STATUS_DB_FETCHED) {
			long updateTime=arg1.getlastFetchTime()+arg1.getFetchInterval();
			if(updateTime<=System.currentTimeMillis()) {
				arg1.setStatus(url_data.STATUS_DB_UNFETCHED);
				arg1.setlastFetchTime(System.currentTimeMillis());
				//arg1.setFetchInterval(defInterval);
				arg2.collect(arg0, arg1);
			}
		}
	}
}
