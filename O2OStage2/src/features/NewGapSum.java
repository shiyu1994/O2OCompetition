package features;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

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

public class NewGapSum {
	private static Boolean onlineOrOffline = false;
	private static Integer threshold = 14;
	
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
			
			if(!dateRec.equals("null")) {
				key.set(0, uid);
				key.set(1, mid);

				counts.set(0, dateRec);

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
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			counts = context.createOutputRecord();
		}
		
		private int parseMonth(String date) {
			return Integer.parseInt(date) / 100 % 100;
		}
		
		private int dateToInt(String date) {
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
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Double[] receivedPerMonth = new Double[7];
			for(int i = 0; i < 7; ++i) 
				receivedPerMonth[i] = 0.0;
			List<String> allReceivedDate = new LinkedList<>();
			//HashSet<String> emitted = new HashSet<>();
			while(values.hasNext()) {
				Record next = values.next();
				
				String dateReceived = next.getString(0);
				int month = parseMonth(dateReceived);
				receivedPerMonth[month - 1] += 1;
				allReceivedDate.add(dateReceived);
			}
			
			Collections.sort(allReceivedDate);
			
			Double getAgainAfterThresholdCount = 0.0;
			Double getAgainBeforeThresholdCount = 0.0; 
			Double nextGapSum = 0.0, prevGapSum = 0.0, minNextGap = 1000.0, minPrevGap = 1000.0;
			Integer nextCount = 0, prevCount = 0;
			
			for(int i = 0; i < allReceivedDate.size(); ++i) {
				String dateString = allReceivedDate.get(i);
				//if(emitted.contains(dateString)) {
				//	continue;
				//}
				//emitted.add(dateString);
				if(i < allReceivedDate.size() - 1 && allReceivedDate.get(i + 1).equals(dateString)) {
					continue;	
				}
				Double repeated = 0.0;
				Integer month = parseMonth(dateString);
				Integer prevDateInt = -1, nextDateInt = -1, prevPrevDateInt = -1, nextNextDateInt = -1;	
				Integer dateInt = dateToInt(dateString);
				Integer nextGap = 1000;
				if(i > 0) {
					prevDateInt = dateToInt(allReceivedDate.get(i - 1));	
					if(dateInt - prevDateInt <= 14) {
						getAgainBeforeThresholdCount += 1;
					}
					prevGapSum += (dateInt - prevDateInt);
					if(dateInt - prevDateInt < minPrevGap) {
						minPrevGap = (double) (dateInt - prevDateInt);
					}
					prevCount += 1;
					if(i > 1) 
						prevPrevDateInt = dateToInt(allReceivedDate.get(i - 2));
				}
				if(i < allReceivedDate.size() - 1) {
					nextDateInt = dateToInt(allReceivedDate.get(i + 1)); 
					if(nextDateInt - dateInt <= 14) {
						getAgainAfterThresholdCount += 1;
					}
					nextGapSum += (nextDateInt - dateInt);
					if(nextDateInt - dateInt < minNextGap) {
						minNextGap = (double) (nextDateInt - dateInt);
					}
					nextCount += 1;
					if(i < allReceivedDate.size() - 2) {
						nextNextDateInt = dateToInt(allReceivedDate.get(i + 2));	
					}
				}
				if(receivedPerMonth[month - 1] > 1.1) {
					repeated = 1.0;
				}
			}
			counts.set(0, key.get(0));
			counts.set(1, key.get(1));
			counts.set(2, getAgainAfterThresholdCount);
			counts.set(3, getAgainBeforeThresholdCount);	
			counts.set(4, prevGapSum / prevCount);
			counts.set(5, nextGapSum / nextCount);
			counts.set(6, prevGapSum);
			counts.set(7, nextGapSum);
			counts.set(8, minPrevGap);
			counts.set(9, minNextGap);
			
			context.write(counts);
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);	
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string,mid:string")); 
		job.setMapOutputValueSchema(SchemaUtils.fromString("dateRec:string"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);	
		
		if(args[0].contains("online")) {
			onlineOrOffline = true;
		}
		else {
			onlineOrOffline = false;
		}
		
		JobClient.runJob(job);
	}
}