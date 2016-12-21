package collaborative.filtering;

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

class CalcScore {
	public static class Mapper extends MapperBase {
		private Record key;
		private Record dates;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			dates = context.createMapOutputValueRecord();
			key.set(new Object[2]);
			dates.set(new Object[3]);
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			String uid = record.getString(0);
			String mid = record.getString(1);
			String daterec = record.getString(5);
			String datebuy = record.getString(6);
			String actOrCid = record.getString(2);
			
			if(Integer.parseInt(mid) > 1000 && actOrCid == "0") {
				return;
			}
			
			key.set(0, uid);
			key.set(1, mid);
			
			dates.set(0, daterec);
			dates.set(1, datebuy);
			
			context.write(key, dates);
		}
	}
	
	public static class Reducer extends ReducerBase {
		private Record scoreRecord;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			scoreRecord = context.createOutputRecord();
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Record value;
			Double score = 0.0;
			while(values.hasNext()) {
				value = values.next();
				String datebuy = value.getString(1);
				String cid = value.getString(2);
				
				if(cid == "null") {
					score += 1;
				}
				else if(datebuy == "null") {
					score += 0;
				}
				else {
					score += 1; 
				}
			}
			scoreRecord.set(0, key.get(0));
			scoreRecord.set(1, key.get(1));
			scoreRecord.set(2, score);
			
			context.write(scoreRecord);
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string,mid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("daterec:string,datebuy:string,cid:string"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);
	}
}