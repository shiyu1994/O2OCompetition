package result;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

class Ensemble {
	public static class Mapper extends MapperBase {
		private Record key, result;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			result = context.createMapOutputValueRecord();
			
			key.set(new Object[1]);
			result.set(new Object[2]);
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			key.set(0, record.get(2));
			result.set(0, record.get(0));
			result.set(1, record.get(1));
			
			context.write(key, result);
		}
	}
	
	public static class Reducer extends ReducerBase {
		private Record result;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			result = context.createOutputRecord();
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			int count = 0;
			Record next;
			double newProbability = 0.0, newLogProb = 0.0;
			Long y = -1L;
			while(values.hasNext()) {
				next = values.next();
				count += 1;
				newProbability += next.getDouble(1);
				newLogProb += Math.log(next.getDouble(1));
				assert(y == -1L || y == next.getBigint(0));
				y = next.getBigint(0);
			}
			newProbability /= count;
			
			result.set(0, y);
			result.set(1, newProbability);
			result.set(2, Math.exp(newLogProb / count));  
			
			context.write(result);
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("append_id:bigint"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("y:bigint,probability:double"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);
	}
}