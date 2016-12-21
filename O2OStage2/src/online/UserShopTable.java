package online;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.Mapper.TaskContext;
import com.aliyun.odps.mapred.JobClient;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.mapred.utils.SchemaUtils;

class UserShopTable {
	
	private static boolean onlineOrOffline = false;
	
	public static class Mapper extends MapperBase {
		private Record key, value;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			value = context.createMapOutputValueRecord();
			key.set(new Object[1]);
			value.set(new Object[1]);
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			String uid = record.getString(0);
			String mid = record.getString(1);
			String cid, act, date;
			if(Integer.parseInt(mid) < 10000) { 
				cid = record.getString(2);
				date = record.getString(6);
				if(!date.equals("null")) {
					key.set(0, uid);
					value.set(0, mid);
					context.write(key, value);
				}
			}
			else {
				cid = record.getString(3);
				date = record.getString(6); 
				act = record.getString(2);
				if(!date.equals("null") && !act.equals("0")) { 	
					key.set(0, uid);
					value.set(0, mid);
					context.write(key, value);
				}
			}
		}
	}
	
	public static class Combiner extends ReducerBase {
		
		private Record key, value;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			value = context.createMapOutputValueRecord();
			key.set(new Object[1]);
			value.set(new Object[1]); 
		}
		
		@Override 
		public void reduce(Record mapperKey, Iterator<Record> values, TaskContext context) throws IOException {
			Record next;
			HashSet<String> offlineShops = new HashSet<>(), onlineShops = new HashSet<>();	
			while(values.hasNext()) {
				next = values.next();
				if(Integer.parseInt(next.getString(0)) < 10000) {	
					offlineShops.add(next.getString(0));
				}
				else {
					onlineShops.add(next.getString(0));
				}
			}
			
			for(String offlineShop : offlineShops) {
				//for(String onlineShop : onlineShops) {
				key.set(0, offlineShop); 
					//value.set(0, onlineShop); 
					//context.write(key, value);
				//}
				value.set(0, onlineShops.size() + "");   
				context.write(key, value); 
				/*for(String offlineShop2 : offlineShops) {	
					if(!offlineShop.equals(offlineShop2)) {
						key.set(0, offlineShop);
						value.set(0, offlineShop2);
						context.write(key, value);
						
						key.set(0, offlineShop2);
						value.set(0, offlineShop); 
						context.write(key, value);  
					}
				}*/
			}
		}
	}
	
	public static class Reducer extends ReducerBase {
		
		private Record output;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			output = context.createOutputRecord();
		}
		
		private class CountItem implements Comparable<CountItem> {
			
			public String mid;
			public Integer count;
			public CountItem(String mid, Integer count) {
				this.mid = mid;
				this.count = count;
			}
			
			@Override
			public int compareTo(CountItem o) {
				if(this.count < o.count) {
					return -1;
				}
				else if(this.count == o.count) {
					return 0;
				}
				else {
					return 1;
				}
			}
			
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			HashMap<String, Integer> counts = new HashMap<>();
			
			Record next;
			while(values.hasNext()) {
				next = values.next();
				if(!counts.containsKey(next.getString(0))) {
					counts.put(next.getString(0), 1);
				}
				else {
					counts.put(next.getString(0), counts.get(next.getString(0)) + 1); 
				}
			}
			
			List<CountItem> countsList = new LinkedList<>();
			for(String mid : counts.keySet()) {
				countsList.add(new CountItem(mid, counts.get(mid)));
			}
			
			Collections.sort(countsList);
			
			Integer countsLen = countsList.size();
			output.set(0, key.get(0));
			output.set(1, countsLen);	
			for(int i = 0; i < 10 && i < countsLen; ++i) {
				output.set(2 * i + 2, countsList.get(countsLen - i - 1).mid);
				output.set(2 * i + 3, countsList.get(countsLen - i - 1).count);
			}
			for(int i = 0; i < 10 - countsLen; ++i) {
				output.set(2 * (countsLen + i) + 2, "null"); 
				output.set(2 * (countsLen + i) + 3, 0); 
			}
			
			context.write(output); 
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		
		job.setMapperClass(Mapper.class);
		job.setCombinerClass(Combiner.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("uid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("mid:string"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job); 
	}
}