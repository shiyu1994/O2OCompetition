package features;

import java.io.IOException;
import java.util.HashMap;
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

class UsedAfter {
	private static boolean onlineOrOffline = false;
	
	private static int dateToInt(String date) {
		int[] monthDays = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
		int intDate = Integer.parseInt(date);
		int month = intDate / 100 % 100;
		int result = 0;
		for(int i = 0; i < month - 1; ++i) {
			result += monthDays[i];
		}
		result += intDate % 100;
		return result;
	}
	
	public static class Mapper extends MapperBase {
		private Record key;
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			key.set(new Object[2]);
			counts = context.createMapOutputValueRecord();
			counts.set(new Object[1]);	
			System.out.println("Task ID: " + context.getTaskID().toString());
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			HashMap<String, String> parsedRecord = parseRecord(record);
			String uid = parsedRecord.get("uid");
			String mid = parsedRecord.get("mid");
			String dateRec = parsedRecord.get("dateRec");
			String dateBuy = parsedRecord.get("date"); 
			
			if(!dateRec.equals("null")) {
				key.set(0, uid);
				key.set(1, mid);

				if(!dateBuy.equals("null"))
					counts.set(0, dateToInt(dateBuy) - dateToInt(dateRec) * 1.0); 
				else 
					counts.set(0, 100.0);	

				context.write(key, counts);
			}
		}
		
		private HashMap<String, String> parseRecord(Record record) {
			HashMap<String, String> result = new HashMap<String, String>();	
			result.put("uid", record.getString(0));
			result.put("mid", record.getString(1));
			if(onlineOrOffline) {
				result.put("cid", record.getString(3));
			}
			else {
				result.put("cid", record.getString(2));
			}
			if(onlineOrOffline) {
				result.put("disRate", record.getString(4));
			}
			else {
				result.put("disRate", record.getString(3));
			}
			if(onlineOrOffline) {
				result.put("dist", "");
			}
			else {
				result.put("dist", record.get(4).toString());
			}
			result.put("dateRec", record.get(5).toString());	
			if(record.getColumnCount() > 6) {
				result.put("date", record.get(6).toString());
			}
			else {
				result.put("date", "null");
			}
			if(onlineOrOffline) {
				result.put("act", record.getString(2));
			}
			else {
				result.put("act", "");
			}
			return result;
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
			Record next;
			Double sum = 0.0, min = 100.0, sumUsed = 0.0;
			Integer count = 0, countUsed = 0;
			while(values.hasNext()) {
				next = values.next();
				Double useAfter = next.getDouble(0);
				sum += useAfter;
				if(useAfter < min) {
					min = useAfter;
				}
				if(useAfter != 100.0) {
					sumUsed += useAfter;
					countUsed += 1;
				}
				count += 1;
			}
			result.set(0, key.get(0));
			result.set(1, key.get(1));
			result.set(2, sum / count);
			result.set(3, sum);
			result.set(4, min);
			result.set(5, sumUsed);
			result.set(6, sumUsed / countUsed); 
			
			context.write(result); 
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string,mid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("gap:double"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);	
	}
}