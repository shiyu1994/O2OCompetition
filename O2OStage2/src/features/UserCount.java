package features;

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

class UserCount {
	static class UserCountMapper extends MapperBase {
		private Record key;
		private Record counts;
		
		@Override
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			counts = context.createMapOutputValueRecord();
			
			key.set(new String[1]);
			counts.set(new Object[4]);

			System.out.println("Task ID: " + context.getTaskID());
		}
		
		@Override
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			
			key.set(0, record.getString(0));
			counts.set(0, record.get(2));
			counts.set(1, record.get(3));
			counts.set(2, record.get(4));
			counts.set(3, record.get(1));
			
			context.write(key, counts);
		}
	}
	
	static class UserCountReducer extends ReducerBase {
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
			Double unusedCoupon = 0.0, buyWithoutCoupon = 0.0, usedCoupon = 0.0;
			while(values.hasNext()) {
				Record next = values.next();
				unusedCoupon += next.getDouble(0);
				buyWithoutCoupon += next.getDouble(1);
				usedCoupon += next.getDouble(2);
			}
			
			Double totalAction = unusedCoupon + buyWithoutCoupon + usedCoupon;	
			Double totalCoupon = unusedCoupon + usedCoupon;
			Double totalBuy = buyWithoutCoupon + usedCoupon;
			
			counts.set(0, key.get(0));
			counts.set(1, unusedCoupon);
			counts.set(2, buyWithoutCoupon);
			counts.set(3, usedCoupon);
			counts.set(4, totalCoupon);
			counts.set(5, totalBuy);
			counts.set(6, divide(unusedCoupon, totalAction));
			counts.set(7, divide(buyWithoutCoupon, totalAction));	
			counts.set(8, divide(usedCoupon, totalAction));
			counts.set(9, divide(usedCoupon, totalCoupon));
			counts.set(10, divide(unusedCoupon, totalCoupon));
			counts.set(11, divide(usedCoupon, totalBuy));
			counts.set(12, divide(buyWithoutCoupon, totalBuy));
			
			context.write(counts); 
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		
		JobConf job = new JobConf();	 
		
		job.setMapperClass(UserCountMapper.class);
		job.setReducerClass(UserCountReducer.class); 
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("unusedCoupon:double,buyWithoutCoupon:double,usedCoupon:double,mid:string")); 
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);
	}
}