package features;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.HashSet;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

class UserDiscount {
	
	private static boolean onlineOrOffline = false;
	
	static class Mapper extends MapperBase {
		private Record key;
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			key.set(new String[1]);
			counts = context.createMapOutputValueRecord();
			counts.set(new Double[6]);	 
			System.out.println("Task ID: " + context.getTaskID().toString());
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			HashMap<String, String> parsedRecord = parseRecord(record);
			String uid = parsedRecord.get("uid");
			String mid = parsedRecord.get("mid");
			String cid = parsedRecord.get("cid");
			String date = parsedRecord.get("date");
			String act = parsedRecord.get("act");
			String disRate = parsedRecord.get("disRate");
			
			Double disRateDouble = 0.0;
			Double lowerBound = -1.0;
			Double disCount = -1.0;
			
			if(disRate.contains(":")) {
				String[] splitted = disRate.split(":");
				lowerBound = Double.parseDouble(splitted[0]);
				disCount = Double.parseDouble(splitted[1]);
				disRateDouble = 1.0 - disCount / lowerBound;
			}
			else if(disRate.equals("null") || disRate.equals("fixed")) {
				lowerBound = -1.0;
				disCount = -1.0;
				disRateDouble = -1.0;
			}
			else {
				disRateDouble = Double.parseDouble(disRate);
				disCount = -1.0;
				lowerBound = -1.0;
			}
			
			key.set(0, uid);
			
			if(date.equals("null") && !cid.equals("null") && (act.equals("") || !act.equals("0"))) {
				counts.set(0, -1.0);
				counts.set(1, -1.0); 
				counts.set(2, -1.0);
				counts.set(3, lowerBound); 
				counts.set(4, disCount);
				counts.set(5, disRateDouble); 
				context.write(key, counts);
			} 
			else if(!date.equals("null") && cid.equals("null") && (act.equals("") || !act.equals("0"))) {
				
			}
			else if(!date.equals("null") && !cid.equals("null") && (act.equals("") || !act.equals("0"))) {
				counts.set(0, lowerBound);
				counts.set(1, disCount); 
				counts.set(2, disRateDouble);
				counts.set(3, -1.0);
				counts.set(4, -1.0);
				counts.set(5, -1.0); 
				context.write(key, counts);
			}
		}
		
		private Integer parseMonth(String date) {
			return Integer.parseInt(date) / 100 % 100;
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
			result.put("date", record.get(6).toString());
			if(onlineOrOffline) {
				result.put("act", record.getString(2));
			}
			else {
				result.put("act", "");
			}
			return result;
		}
	}
	
	static class Reducer extends ReducerBase {
		private Record counts;
		
		private Double divide(Double x, Double y) {
			if(y <= 1e-5 && y >= -1e-5) {
				return 0.0;
			}
			else {
				return x / y;
			}
		}
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			counts = context.createOutputRecord();
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Double disRateSum = 0.0, lowerBoundSum = 0.0, disCountSum = 0.0, 
					disRateMin = 1000.0, disRateMax = 0.0, 
					disCountMin = 1000.0, disCountMax = 0.0,
					lowerBoundMin = 1000.0, lowerBoundMax = 0.0,
					unusedDisRateSum = 0.0, unusedLowerBoundSum = 0.0, unusedDisCountSum = 0.0, 
					unusedDisRateMin = 1000.0, unusedDisRateMax = 0.0, 
					unusedLowerBoundMin = 1000.0, unusedLowerBoundMax = 0.0,
					unusedDisCountMin = 1000.0, unusedDisCountMax = 0.0;
			
			Integer disRateCount = 0, lowerBoundCount = 0, disCountCount = 0, 
					unusedDisRateCount = 0, unusedLowerBoundCount = 0, unusedDisCountCount = 0;
			
			HashSet<Double> usedLowerBounds = new HashSet<>();
			HashSet<Double> usedDiscounts = new HashSet<>();
			HashSet<Double> usedDisRates = new HashSet<>();
			
			Record next;
			while(values.hasNext()) {
				next = values.next();
				Double usedLowerBound = next.getDouble(0);
				Double usedDiscount = next.getDouble(1);
				Double usedDisRate = next.getDouble(2);
				
				usedLowerBounds.add(usedLowerBound);
				usedDiscounts.add(usedDiscount);
				usedDisRates.add(usedDisRate);
				
				Double unusedLowerBound = next.getDouble(3);	
				Double unusedDiscount = next.getDouble(4);
				Double unusedDisRate = next.getDouble(5);
				
				if(usedLowerBounds.contains(unusedLowerBound)) {
					unusedLowerBound = -1.0;
				}
				if(usedDiscounts.contains(unusedDiscount)) {
					unusedDiscount = -1.0;
				}
				if(usedDisRates.contains(unusedDisRate)) {
					unusedDisRate = -1.0;
				}
				
				if(usedLowerBound != -1.0) {
					lowerBoundSum += usedLowerBound;
					lowerBoundCount += 1;
					if(usedLowerBound < lowerBoundMin) {
						lowerBoundMin = usedLowerBound;
					}
					if(usedLowerBound > lowerBoundMax) {
						lowerBoundMax = usedLowerBound;
					}
 				}
				
				if(usedDiscount != -1.0) {
					disCountSum += usedDiscount;
					disCountCount += 1;
					if(usedDiscount < disCountMin) {
						disCountMin = usedDiscount;
					}
					if(usedDiscount > disCountMax) {
						disCountMax = usedDiscount;
					}
				}
				
				if(usedDisRate != -1.0) {
					disRateSum += usedDisRate; 
					disRateCount += 1;
					if(usedDisRate < disRateMin) {
						disRateMin = usedDisRate;
					}
					if(usedDisRate > disRateMax) {
						disRateMax = usedDisRate;
					}
				}
				
				if(unusedLowerBound != -1.0) {
					unusedLowerBoundSum += unusedLowerBound;
					unusedLowerBoundCount += 1;
					if(unusedLowerBound < unusedLowerBoundMin) {
						unusedLowerBoundMin = unusedLowerBound;
					}
					if(unusedLowerBound > unusedLowerBoundMax) {
						unusedLowerBoundMax = unusedLowerBound;
					}
 				}
				
				if(unusedDiscount != -1.0) {
					unusedDisCountSum += unusedDiscount;
					unusedDisCountCount += 1;
					if(unusedDiscount < unusedDisCountMin) {
						unusedDisCountMin = unusedDiscount;
					}
					if(unusedDiscount > unusedDisCountMax) {
						unusedDisCountMax = unusedDiscount;
					}
				}
				
				if(unusedDisRate != -1.0) {
					unusedDisRateSum += unusedDisRate; 
					unusedDisRateCount += 1;
					if(unusedDisRate < unusedDisRateMin) {
						unusedDisRateMin = unusedDisRate;
					}
					if(unusedDisRate > unusedDisRateMax) {
						unusedDisRateMax = unusedDisRate;
					}
				}
			}
			
			counts.set(0, key.get(0));
			counts.set(1, lowerBoundSum / lowerBoundCount);
			counts.set(2, lowerBoundMin);
			counts.set(3, lowerBoundMax);
			
			counts.set(4, disCountSum / disCountCount);
			counts.set(5, disCountMin);
			counts.set(6, disCountMax);
			
			counts.set(7, disRateSum / disRateCount);
			counts.set(8, disRateMin);
			counts.set(9, disRateMax);
			
			counts.set(10, unusedLowerBoundSum / unusedLowerBoundCount);
			counts.set(11, unusedLowerBoundMin);
			counts.set(12, unusedLowerBoundMax);
			
			counts.set(13, unusedDisCountSum / unusedDisCountCount);
			counts.set(14, unusedDisCountMin);
			counts.set(15, unusedDisCountMax);
			
			counts.set(16, unusedDisRateSum / unusedDisRateCount);
			counts.set(17, unusedDisRateMin);
			counts.set(18, unusedDisRateMax);	
			
			context.write(counts); 
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();	 
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);  
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("d0:double,d1:double,d2:double,d3:double,d4:double,d5:double")); 
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);
	}
}