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
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

class UserCountOnline {
	
	static class Mapper extends MapperBase {
		private Record key;
		private Record counts;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			key.set(new String[1]);
			counts = context.createMapOutputValueRecord();
			counts.set(new Object[5]);	
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
			
			Double unusedCoupon = 0.0, buyWithoutCoupon = 0.0, usedCoupon = 0.0, click = 0.0;
			if(act.equals("0")) {
				click = 1.0;
			} 
			else if(cid.equals("null") && !date.equals("null")) {
				buyWithoutCoupon = 1.0;
			}
			else if(!cid.equals("null") && date.equals("null")) {
				unusedCoupon = 1.0;
			}
			else {
				assert(!cid.equals("null") && !date.equals("null"));
				usedCoupon = 1.0;
			}
			key.set(0, uid);
			counts.set(0, click);
			counts.set(1, unusedCoupon);
			counts.set(2, buyWithoutCoupon);
			counts.set(3, usedCoupon);
			counts.set(4, (parsedRecord.get("month")));   
			context.write(key, counts);
		}
		
		private Integer parseMonth(String date) {
			return Integer.parseInt(date) / 100 % 100;
		}
		
		private HashMap<String, String> parseRecord(Record record) {
			HashMap<String, String> result = new HashMap<String, String>();
			result.put("uid", record.getString(0));
			result.put("mid", record.getString(1));
			
			result.put("cid", record.getString(3));
			
			result.put("disRate", record.getString(4));
			
			result.put("dist", "");
			
			result.put("dateRec", record.get(5).toString());	
			result.put("date", record.get(6).toString());
			
			result.put("act", record.getString(2));
			
			if(!result.get("dateRec").equals("null")) {
				result.put("month", parseMonth(result.get("dateRec")).toString());
			}
			else {
				result.put("month", parseMonth(result.get("date")).toString()); 
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
		
		private Double calcTrend(Double[] list, int n) {
			if(n == 1) {
				return 0.0;
			}
			else {
				Double sum_x_2 = 0.0;
				for(int i = 0; i < n; ++i) {
					sum_x_2 += i * i;
				}
				Double mean_x = (n - 1.0) / 2;
				Double sum_x_y = 0.0;
				for(int i = 0; i < n; ++i) {
					sum_x_y += i * list[i];
				}
				Double mean_y = 0.0;
				for(int i = 0; i < n; ++i) {
					mean_y += list[i];
				}
				mean_y /= n;
				return (sum_x_y - n * mean_x * mean_y) * 1.0 / (sum_x_2 - n * (mean_x * mean_x));
			}
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Double click = 0.0, unusedCoupon = 0.0, buyWithoutCoupon = 0.0, usedCoupon = 0.0;
			
			Double[] clickList = new Double[6], unusedCouponList = new Double[6], buyWithoutCouponList = new Double[6], usedCouponList = new Double[6];
			int n = 0;
			for(int i = 0; i < 6; ++i) {
				clickList[i] = 0.0;
				unusedCouponList[i] = 0.0;
				buyWithoutCouponList[i] = 0.0;
				usedCouponList[i] = 0.0;
			}
			
			while(values.hasNext()) {
				Record next = values.next();
				
				Integer month = Integer.parseInt(next.getString(4)); 
				
				if(month > n) {
					n = month;
				}
				
				clickList[month - 1] += next.getDouble(0);
				unusedCouponList[month - 1] += next.getDouble(1);
				buyWithoutCouponList[month - 1] += next.getDouble(2);
				usedCouponList[month - 1] += next.getDouble(3);
				
				click += next.getDouble(0);
				unusedCoupon += next.getDouble(1);
				buyWithoutCoupon += next.getDouble(2);
				usedCoupon += next.getDouble(3);
			}
			
			Double totalAction = click + unusedCoupon + buyWithoutCoupon + usedCoupon;	
			Double totalCoupon = unusedCoupon + usedCoupon;
			Double totalBuy = buyWithoutCoupon + usedCoupon;
			
			counts.set(0, key.get(0));
			counts.set(1, click);
			counts.set(2, unusedCoupon);
			counts.set(3, buyWithoutCoupon);
			counts.set(4, usedCoupon);
			counts.set(5, totalCoupon);
			counts.set(6, totalBuy);
			counts.set(7, divide(click, totalAction));
			counts.set(8, divide(unusedCoupon, totalAction));
			counts.set(9, divide(buyWithoutCoupon, totalAction));	
			counts.set(10, divide(usedCoupon, totalAction));
			counts.set(11, divide(usedCoupon, totalCoupon));
			counts.set(12, divide(unusedCoupon, totalCoupon));
			counts.set(13, divide(usedCoupon, totalBuy));
			counts.set(14, divide(buyWithoutCoupon, totalBuy));
			
			counts.set(15, calcTrend(clickList, n));
			counts.set(16, calcTrend(unusedCouponList, n));
			counts.set(17, calcTrend(buyWithoutCouponList, n));
			counts.set(18, calcTrend(usedCouponList, n));  
			
			Double[] totalActList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				totalActList[i] = clickList[i] + unusedCouponList[i] + buyWithoutCouponList[i] + usedCouponList[i];
			}
			
			Double[] totalCouponList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				totalCouponList[i] = unusedCouponList[i] + usedCouponList[i];
			}
			
			counts.set(16, calcTrend(totalCouponList, n));
			
			Double[] totalBuyList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				totalBuyList[i] = buyWithoutCouponList[i] + usedCouponList[i];
			}
			
			counts.set(17, calcTrend(totalBuyList, n));
			
			Double[] act0RatioList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				act0RatioList[i] = divide(clickList[i], totalActList[i]);
			}
			
			counts.set(18, calcTrend(act0RatioList, n));
			
			Double[] act1RatioList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				act1RatioList[i] = divide(unusedCouponList[i], totalActList[i]);
			}
			
			counts.set(19, calcTrend(act1RatioList, n));
			
			Double[] act2RatioList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				act2RatioList[i] = divide(buyWithoutCouponList[i], totalActList[i]);	
			}
			
			counts.set(21, calcTrend(act2RatioList, n));
			
			Double[] act3RatioList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				act3RatioList[i] = divide(usedCouponList[i], totalActList[i]);
			}
			
			counts.set(22, calcTrend(act3RatioList, n));
			
			Double[] usedCouponRatioList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				usedCouponRatioList[i] = usedCouponList[i] / totalCouponList[i];
			}
			
			counts.set(23, calcTrend(usedCouponRatioList, n));
			
			Double[] buyWithCouponRatioList = new Double[6];
			for(int i = 0; i < 6; ++i) {
				buyWithCouponRatioList[i] = usedCouponList[i] / totalBuyList[i];
			}
			
			counts.set(24, calcTrend(buyWithCouponRatioList, n));
			
			context.write(counts); 
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();	 
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class); 
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("click:double,unusedCoupon:double,buyWithoutCoupon:double,usedCoupon:double,month:string")); 
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);
	}
}