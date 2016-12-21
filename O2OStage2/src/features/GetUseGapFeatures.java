package features;

import java.io.IOException;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.mapred.MapperBase;

class GetUseGapFeatures {
	public static class Mapper extends MapperBase {
		private Record key, value;
		
		@Override 
		public void setup(TaskContext context) throws IOException {
			key = context.createMapOutputKeyRecord();
			value = context.createMapOutputValueRecord();
		}
	}
}