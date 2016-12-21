package collaborative.filtering;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.mapred.MapperBase;
import com.aliyun.odps.mapred.Reducer.TaskContext;
import com.aliyun.odps.mapred.ReducerBase;

class BuildTable {
	public static class Mapper extends MapperBase {
		private Record key, value;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			key.set(new Object[1]);
			value = context.createMapOutputValueRecord();
			value.set(new Object[2]);
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			key.set(0, record.getString(0));
			value.set(0, record.getString(1));
			value.set(1, record.getDouble(2));
			context.write(key, value);
		}
	}
	
	public static class Combiner extends ReducerBase {
		private Record output;
		private Record midKey;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			midKey = context.createOutputKeyRecord();
			midKey.set(new Object[1]);
			output = context.createOutputValueRecord();
			output.set(new Object[2]);
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			HashMap<String, Double> scores = new HashMap<>();
			
			Record next;
			while(values.hasNext()){
				next = values.next();
				String mid = next.getString(1);
				Double score = next.getDouble(2);
				
				scores.put(mid, score);
			}
			
			for(String mid : scores.keySet()) {
				midKey.set(0, mid);
				output.set(0, key.get(0));
				output.set(1, scores);
				context.write(midKey, output);
			}
		}
	}
	
	public static class Reducer extends ReducerBase {
		private Record output;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			output = context.createMapOutputValueRecord();
			output.set(new Object[60002]);
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Record next;
			Double[] scores = new Double[60001];
			HashSet<String> users = new HashSet<>();
			for(int i = 0; i < 60001; ++i) {
				scores[i] = 0.0;
			}
			int count = 0;
			while(values.hasNext()) {
				next = values.next();
				HashMap<String, Double> map = (HashMap<String, Double>)next.get(1);
				users.add(next.getString(0));
				for(String mid : map.keySet()) {
					scores[Integer.parseInt(mid)] += map.get(mid);
				}
				count += 1;
			}
			for(int i = 0; i < 60001; ++i) {
				scores[i] /= count;
			}
			for(String uid : users) {
				//TODO
			}
		}
	}
}