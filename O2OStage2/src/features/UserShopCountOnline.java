package features;

import java.io.IOException;
import java.util.Iterator;
import java.util.HashMap;

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

public class UserShopCountOnline {
	
	public static class Mapper extends MapperBase {
		private Record key;
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			key.set(new String[2]);
			counts = context.createMapOutputValueRecord();
			counts.set(new Double[3]);	
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
			
			Double unusedCoupon = 0.0, buyWithoutCoupon = 0.0, usedCoupon = 0.0;
			if(date.equals("null") && !cid.equals("null") && (act.equals("") || !act.equals("0"))) {
				unusedCoupon = 1.0;
				buyWithoutCoupon = 0.0;
				usedCoupon = 0.0;
			} 
			else if(!date.equals("null") && cid.equals("null") && (act.equals("") || !act.equals("0"))) {
				unusedCoupon = 0.0;
				buyWithoutCoupon = 1.0;
				usedCoupon = 0.0;
			}
			else if(!date.equals("null") && !cid.equals("null") && (act.equals("") || !act.equals("0"))) {
				unusedCoupon = 0.0;
				buyWithoutCoupon = 0.0;
				usedCoupon = 1.0;
			}
			else {
				return;
			}
			key.set(0, uid);
			key.set(1, mid);
			counts.set(0, unusedCoupon);
			counts.set(1, buyWithoutCoupon);
			counts.set(2, usedCoupon);
			context.write(key, counts);
		}
		
		private HashMap<String, String> parseRecord(Record record) {
			HashMap<String, String> result = new HashMap<String, String>();
			result.put("uid", record.getString(0));
			result.put("mid", record.getString(1));
			result.put("act", record.getString(2));
			result.put("cid", record.getString(3));
			result.put("disRate", record.getString(4));
			result.put("dateRec", record.get(5).toString());	
			
			if(record.getColumnCount() > 6) {
				result.put("date", record.get(6).toString());
			} 
			else {
				result.put("date", "null");
			}
			
			return result;
		}
	}
	
	public static class Reducer extends ReducerBase {
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
			Double unusedCoupon = 0.0;
			Double buyWithoutCoupon = 0.0;
			Double usedCoupon = 0.0;
			while(values.hasNext()) {
				Record next = values.next();
				
				unusedCoupon += next.getDouble(0);
				buyWithoutCoupon += next.getDouble(1);
				usedCoupon += next.getDouble(2); 
			}
			
			Double totalActions = unusedCoupon + buyWithoutCoupon + usedCoupon;
			Double totalCoupon = unusedCoupon + usedCoupon;
			Double totalBuy = buyWithoutCoupon + usedCoupon; 
			
			counts.set(0, key.get(0));
			counts.set(1, key.get(1));
			counts.set(2, unusedCoupon);
			counts.set(3, buyWithoutCoupon);
			counts.set(4, usedCoupon);
			counts.set(5, totalCoupon);
			counts.set(6, totalBuy);
			counts.set(7, divide(unusedCoupon, totalActions));
			counts.set(8, divide(buyWithoutCoupon, totalActions));
			counts.set(9, divide(usedCoupon, totalActions));
			counts.set(10, divide(usedCoupon, totalCoupon));
			counts.set(11, divide(unusedCoupon, totalCoupon));
			counts.set(12, divide(usedCoupon, totalBuy));
			counts.set(13, divide(buyWithoutCoupon, totalBuy)); 
			
			context.write(counts);
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string,mid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("unusedCoupon:double,buyWithoutCoupon:double,usedCoupon:double"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);	
		
		JobClient.runJob(job);
	}
}