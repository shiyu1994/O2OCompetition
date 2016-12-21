package clustering;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

class UserFeatures {
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
		public void map(long recordNumber, Record record, TaskContext context) {
			
		}
	}
}