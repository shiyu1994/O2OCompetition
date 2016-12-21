package result;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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

class CalcAvgAuc {
	public static class Mapper extends MapperBase {
		private Record key, value;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			value = context.createMapOutputValueRecord();
			key.set(new Object[1]);
			value.set(new Object[2]);
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) throws IOException {
			String cid = record.getString(1);
			Long y = record.getBigint(0); 
			String detail = record.getString(4);
			String[] splitted = detail.split(":|,|\\}"); 
			Double score = Double.parseDouble(splitted[3]);
			
			key.set(0, cid);
			value.set(0, y);
			value.set(1, score);
			
			context.write(key, value); 
		}
	}
	
	public static class Reducer extends ReducerBase {
		private Record result;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			result = context.createOutputRecord();
		}
		
		private class Pair implements Comparable<Pair> {
			public Long y;
			public Double score;
			
			public Pair(Long y, Double score) {
				this.y = y;
				this.score = score;
			}
			
			@Override
			public int compareTo(Pair other) {
				if(this.score < other.score)
					return -1;
				else if(this.score > other.score)
					return 1;
				else
					return 0;
			}
		}
		
		@Override 
		public void reduce(Record key, Iterator<Record> values, TaskContext context) throws IOException {
			Record next;
			List<Pair> records = new LinkedList<>();
			Integer negative = 0, positive = 0;
			while(values.hasNext()) {
				next = values.next();
				Long y = next.getBigint(0);
				if(y == 0L) {
					negative += 1;
				}
				else {
					positive += 1;
				}
				records.add(new Pair(y, next.getDouble(1)));
			}
			if(negative == 0 || positive == 0) {
				return;
			}
			Collections.sort(records);
			
			Integer truePos = positive, falsePos = negative;
			List<Double[]> coordinates = new LinkedList<>();
			for(int i = 0; i < records.size(); ++i) {
				Pair pair = records.get(i);
				Long y = pair.y;
				if(y == 1L) {
					truePos -= 1;
				}
				else {
					falsePos -= 1;
				}
				Double[] coordinate = new Double[2];
				coordinate[1] = truePos * 1.0 / positive;
				coordinate[0] = falsePos * 1.0 / negative;
				coordinates.add(coordinate);
			}
			Double area = 0.0;
			Integer coordinatesNumber = coordinates.size();
			for(int i = 0; i < coordinatesNumber; ++i) {
				Double[] coordinate = coordinates.get(i);
				if(i == 0) {
					area += (1.0 - coordinate[0]) * (coordinate[1] + 1.0) / 2;	
				}
				else {
					Double[] prev = coordinates.get(i - 1);
					area += (prev[0] - coordinate[0]) * (prev[1] + coordinate[1]) / 2;
				}
			}
			Double[] lastCoordinate = coordinates.get(coordinatesNumber - 1);
			area += lastCoordinate[0] * lastCoordinate[1] / 2;
			
			result.set(0, key.get(0));
			result.set(1, area);
			context.write(result);
		}
	}
	
	public static void main(String[] args) throws OdpsException {
		JobConf job = new JobConf();
		
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		
		job.setMapOutputKeySchema(SchemaUtils.fromString("cid:string"));
		job.setMapOutputValueSchema(SchemaUtils.fromString("y:bigint,score:double"));
		
		InputUtils.addTable(TableInfo.builder().tableName(args[0]).build(), job);
		OutputUtils.addTable(TableInfo.builder().tableName(args[1]).build(), job);
		
		JobClient.runJob(job);
	}
}