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

public class BasicFeatures {
	private static Boolean onlineOrOffline = false;
	
	public static class BasicFeaturesMapper extends MapperBase {
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			counts = context.createOutputRecord();
			System.out.println("Task ID: " + context.getTaskID().toString());
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			HashMap<String, String> parsedRecord = parseRecord(record);
			String disRate = parsedRecord.get("disRate");
			String dist = parsedRecord.get("dist");
			String cid = parsedRecord.get("cid");
			String dateBuy = parsedRecord.get("date");
			
			Double disRateDouble = null; 
			Double lowerBound = null;
			Double disCount = null;
			Double distDouble = null;
			
			if(disRate.contains(":")) {
				String[] splitted = disRate.split(":");
				lowerBound = Double.parseDouble(splitted[0]);
				disCount = Double.parseDouble(splitted[1]);
				disRateDouble = 1.0 - disCount / lowerBound;
			}
			else if(disRate.equals("null") || disRate.equals("fixed")) {
				
			}
			else {
				disRateDouble = Double.parseDouble(disRate);
				//disCount = -1.0;
				//lowerBound = -1.0;	
			}
			
			if(!dist.equals("null")) {
				distDouble = Double.parseDouble(dist); 
			}
			
			int recordLength = record.getColumnCount();
			for(int i = 0; i < recordLength; ++i) {
				counts.set(i, record.getString(i));
			}
			if(recordLength == 6) {
				counts.set(6, "null");
				recordLength += 1;
			}
			counts.set(recordLength, disRateDouble);
			counts.set(recordLength + 1, lowerBound);
			counts.set(recordLength + 2, disCount);
			counts.set(recordLength + 3, distDouble);
			
			if(!cid.equals("null")) {
				if(dateBuy.equals("null")) {
					counts.set(recordLength + 4, 0);
				}
				else {
					counts.set(recordLength + 4, 1);	
				}
				context.write(counts);
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
				result.put("dist", "null");
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
				result.put("act", "null");
			}
			return result;
		}
	}
	
	public static class BasicFeaturesReducer extends ReducerBase { 
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
				result += monthDays[i - 1];
			}
			result += intDate % 100;
			return result;
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Double[] receivedPerMonth = new Double[6];
			for(int i = 0; i < 6; ++i) 
				receivedPerMonth[i] = 0.0;
			List<String> allReceivedDate = new LinkedList<>();
			HashSet<String> emitted = new HashSet<>();
			while(values.hasNext()) {
				Record next = values.next();
				
				String dateReceived = next.getString(0);
				int month = parseMonth(dateReceived);
				receivedPerMonth[month] += 1;
				allReceivedDate.add(dateReceived);
			}
			
			Collections.sort(allReceivedDate);
			
			for(int i = 0; i < allReceivedDate.size(); ++i) {
				String dateString = allReceivedDate.get(i);
				if(emitted.contains(dateString)) {
					continue;
				}
				emitted.add(dateString);
				Double repeated = 0.0;
				Integer month = parseMonth(dateString);
				Integer prevDateInt = -1, nextDateInt = -1;	
				Integer dateInt = dateToInt(dateString);
				if(i > 0) {
					prevDateInt = dateToInt(allReceivedDate.get(i - 1));		
				}
				if(i < allReceivedDate.size() - 1) {
					nextDateInt = dateToInt(allReceivedDate.get(i + 1)); 
				}
				if(receivedPerMonth[i] > 0.1) {
					repeated = 1.0;
				}
				counts.set(0, key.get(0));
				counts.set(1, key.get(1));
				counts.set(2, dateString);
				counts.set(3, repeated);
				counts.set(4, receivedPerMonth[month]);
				if(prevDateInt == -1)
					counts.set(5, 1000.0);
				else
					counts.set(5, (dateInt - prevDateInt) * 1.0);
				if(nextDateInt == -1)
					counts.set(6, 1000.0);
				else
					counts.set(6, (nextDateInt - dateInt) * 1.0);
				context.write(counts);
			}
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();
		job.setMapperClass(BasicFeaturesMapper.class);	
		
		job.setMapOutputValueSchema(SchemaUtils.fromString("uid:string,mid:string,cid:string,disRate:string,dist:string,dateRec:string,dateBuy:string,disRateDouble:double,lowerBound:double,disCount:double,distDouble:double,y:int"));
		
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