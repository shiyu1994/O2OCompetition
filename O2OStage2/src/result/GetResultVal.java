package result;

import java.io.IOException;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;

public class GetResultVal {	
	public static class BasicFeaturesMapper extends MapperBase {
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			counts = context.createOutputRecord();
			System.out.println("Task ID: " + context.getTaskID().toString()); 
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			String detail = record.getString(record.getColumnCount() - 1);
			String[] splitted = detail.split(":|,|\\}"); 
			Double score = Double.parseDouble(splitted[3]);
			counts.set(0, record.get(0));
			counts.set(1, score);	
			counts.set(2, record.get(1));
			context.write(counts); 
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();
		job.setMapperClass(BasicFeaturesMapper.class);	
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);	
		
		JobClient.runJob(job);
	}
}