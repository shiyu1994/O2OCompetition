package online;

import java.io.IOException;
import java.util.Iterator;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

class CalcScore {
	public static class Mapper extends MapperBase {
		private Record output;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			output = context.createOutputRecord();
		}
		
		@Override 
		public void map(long recordNumber, Record record, TaskContext context) {
			
		}
	}
}