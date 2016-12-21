package online;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.Mapper.TaskContext;

import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.ReducerBase;

class UserTable {
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
			if(Integer.parseInt(mid) < 10000) {
				String date = record.getString(6);
				if(!date.equals("null")) {
					key.set(0, uid);
					value.set(0, mid);
					context.write(key, value);    
				}
			}
			else {
				String date = record.getString(6);
				String act = record.getString(2);
				if(!date.equals("null") && !act.equals("0")) {
					key.set(0, uid);
					value.set(0, mid);
					context.write(key, value); 
				}
			}
		}
	}
	
	public static class Reducer extends ReducerBase {
		private Record output;
		
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
		public void setup(TaskContext context) throws IOException {
			output = context.createOutputRecord();
		}
		
		@Override
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Record next;
			HashMap<String, Integer> counts = new HashMap<>(); 
			while(values.hasNext()) {
				next = values.next();
				String mid = next.getString(0);
				if(!counts.containsKey(mid)) {
					counts.put(mid, 1);
				}
				else {
					counts.put(mid, counts.get(mid) + 1);
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
}