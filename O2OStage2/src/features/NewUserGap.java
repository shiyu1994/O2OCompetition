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

public class NewUserGap {
	private static Boolean onlineOrOffline = false;
	
	public static class Mapper extends MapperBase {
		private Record key;
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			key.set(new Object[1]);
			counts = context.createMapOutputValueRecord();
			counts.set(new Object[1]);	
			System.out.println("Task ID: " + context.getTaskID().toString());
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			HashMap<String, String> parsedRecord = parseRecord(record);
			String uid = parsedRecord.get("uid");
			String dateRec = parsedRecord.get("dateRec");
			
			if(!dateRec.equals("null")) {
				key.set(0, uid);

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
			HashSet<String> emitted = new HashSet<>();
			while(values.hasNext()) {
				Record next = values.next();
				
				String dateReceived = next.getString(0);
				int month = parseMonth(dateReceived);
				receivedPerMonth[month - 1] += 1;
				allReceivedDate.add(dateReceived);
			}
			
			Collections.sort(allReceivedDate);
			
			for(int i = 0; i < allReceivedDate.size(); ++i) {
				String dateString = allReceivedDate.get(i);
				//if(emitted.contains(dateString)) {
				//	continue;
				//}
				if(i < allReceivedDate.size() - 1 && allReceivedDate.get(i + 1).equals(dateString)) {
					continue;	
				}
				emitted.add(dateString);
				Double repeated = 0.0;
				Integer month = parseMonth(dateString);
				Integer prevDateInt = -1, nextDateInt = -1, prevPrevDateInt = -1, nextNextDateInt = -1;	
				Integer dateInt = dateToInt(dateString);
				Integer nextGap = 1000;
				if(i > 0) {
					prevDateInt = dateToInt(allReceivedDate.get(i - 1));	
					if(i > 1)
						prevPrevDateInt = dateToInt(allReceivedDate.get(i - 2));
				}
				if(i < allReceivedDate.size() - 1) {
					nextDateInt = dateToInt(allReceivedDate.get(i + 1)); 
					if(i < allReceivedDate.size() - 2) {
						nextNextDateInt = dateToInt(allReceivedDate.get(i + 2));
					}
				}
				if(receivedPerMonth[month - 1] > 1.1) {
					repeated = 1.0;
				}
				counts.set(0, key.get(0));
				counts.set(1, dateString);
				counts.set(2, repeated);
				counts.set(3, receivedPerMonth[month - 1]);	
				if(prevDateInt == -1)	
					counts.set(4, null);
				else
					counts.set(4, (dateInt - prevDateInt) * 1.0);
				if(nextDateInt == -1)
					counts.set(5, null);
				else {
					nextGap = nextDateInt - dateInt;
					counts.set(5, nextGap * 1.0);
				}
				if(i == 0) {
					counts.set(6, 1.0);
				}
				else {
					counts.set(6, 0.0); 
				}
				
				Double getPrevious31Days = 0.0;
				Double getNext31Days = 0.0;
				
				for(int j = i - 1; j >= 0; --j) {
					int gap = dateInt - dateToInt(allReceivedDate.get(j));
					if(gap <= 31) {
						getPrevious31Days += 1.0;
					}
					else {
						break;
					}
				}
				for(int j = i + 1; j < allReceivedDate.size(); ++j) {
					int gap = dateToInt(allReceivedDate.get(j)) - dateInt;	
					if(gap <= 31) {
						getNext31Days += 1.0;
					}
					else {
						break;
					}
				}
				
				counts.set(7, getPrevious31Days);
				
				counts.set(8, getNext31Days);
				
				Double getPrevious15Days = 0.0;
				Double getNext15Days = 0.0;
				
				for(int j = i - 1; j >= 0; --j) {
					int gap = dateInt - dateToInt(allReceivedDate.get(j));
					if(gap <= 15) {
						getPrevious15Days += 1.0;
					}
					else {
						break;
					}
				}
				for(int j = i + 1; j < allReceivedDate.size(); ++j) {
					int gap = dateToInt(allReceivedDate.get(j)) - dateInt;
					if(gap <= 15) {
						getNext15Days += 1.0;
					}
					else {
						break;
					}
				}
				
				counts.set(9, getPrevious15Days);
				counts.set(10, getNext15Days);
				
				counts.set(11, getPrevious15Days + getNext15Days);
				
				if(prevPrevDateInt == -1) {
					counts.set(12, null);
				}
				else {
					counts.set(12, (dateInt - prevPrevDateInt) * 1.0);
				}
				
				if(nextNextDateInt == -1) {
					counts.set(13, null); 
				}
				else {
					counts.set(13, (nextNextDateInt - dateInt) * 1.0);
				}
				
				if(nextGap == 0) {
					counts.set(14, 1.0);
				}
				else {
					counts.set(14, 0.0);
				}
				
				if(nextGap != 1000) {
					counts.set(15, Math.sqrt(nextGap));
					counts.set(16, nextGap * nextGap * 1.0);
				}
				else {
					counts.set(15, null);
					counts.set(16, null);
				}
				
				counts.set(17, Math.sqrt(receivedPerMonth[month - 1]));
				counts.set(18, receivedPerMonth[month - 1] * receivedPerMonth[month - 1]);	
				
				context.write(counts);
			}
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string"));	 
		job.setMapOutputValueSchema(SchemaUtils.fromString("dateRec:string"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);	
		
		JobClient.runJob(job);
	}
}