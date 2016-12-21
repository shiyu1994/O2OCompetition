package collaborative.filtering;

import java.io.IOException;

import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.graph.ComputeContext;
import com.aliyun.odps.graph.Edge;
import com.aliyun.odps.graph.GraphJob;
import com.aliyun.odps.graph.GraphLoader;
import com.aliyun.odps.graph.MutationContext;
import com.aliyun.odps.graph.Vertex;
import com.aliyun.odps.graph.WorkerContext;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.io.WritableRecord;
import com.aliyun.odps.io.DoubleWritable;
import com.aliyun.odps.io.LongWritable;
import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.IntWritable;
import java.util.HashMap;

class CollaborativeFiltering {
	
	static private int featureLength = 10;
	static private double learningRate = 0.01;
	static private double reg = 0.0;
	
	private static ArrayWritable getDefaultArrayWritable() {
		DoubleWritable[] dw = new DoubleWritable[featureLength];
		for(int i = 0; i < featureLength; ++i) {
			dw[i] = new DoubleWritable(0.01 * Math.random());
		}
		ArrayWritable aw = new ArrayWritable(DoubleWritable.class);
		aw.set(dw);
		return aw;
	}
	
	public static class CFVertex extends Vertex<IntWritable, ArrayWritable, DoubleWritable, ArrayWritable> {

		@Override
		public void compute(ComputeContext<IntWritable, ArrayWritable, DoubleWritable, ArrayWritable> context,
				Iterable<ArrayWritable> messages) throws IOException {
			if(context.getSuperstep() == 0) {
				this.setValue(getDefaultArrayWritable()); 
			}
			else {
				Double[] grads = new Double[featureLength];
				HashMap<Integer, ArrayWritable> messagesIn = new HashMap<>();
				for(ArrayWritable msg : messages) {
					Writable[] doubles = msg.get();
					Integer sourceId = (int) ((DoubleWritable)doubles[featureLength]).get();
					messagesIn.put(sourceId, msg);
				}
				
				Writable[] features = this.getValue().get();
				for(Edge<IntWritable, DoubleWritable> edge : this.getEdges()) {
					Integer sourceId = edge.getDestVertexId().get();
					Writable[] doubles = messagesIn.get(sourceId).get();
					Double product = 0.0;
					Double score = edge.getValue().get();
					for(int i = 0; i < featureLength; ++i) {
						product += ((DoubleWritable)doubles[i]).get() * ((DoubleWritable)features[i]).get();
					}
					for(int i = 0; i < featureLength; ++i) {
						grads[i] += 2 * (product - score) * ((DoubleWritable)doubles[i]).get();
					}
				}
				
				ArrayWritable newFeatures = new ArrayWritable(DoubleWritable.class);
				DoubleWritable[] dw = new DoubleWritable[featureLength];
				for(int i = 0; i < featureLength; ++i) {
					dw[i] = new DoubleWritable(((DoubleWritable)features[i]).get() - learningRate * grads[i] - 2 * reg * ((DoubleWritable)features[i]).get());
				}
				newFeatures.set(dw);
				this.setValue(newFeatures);				
				
				ArrayWritable messageOut = new ArrayWritable(DoubleWritable.class); 
				Writable[] mo = new Writable[featureLength + 1];
				for(int i = 0; i < featureLength; ++i) {
					mo[i] = dw[i];
				}
				mo[featureLength] = new DoubleWritable(this.getId().get() * 1.0);
				messageOut.set(mo);
				
				for(Edge<IntWritable, DoubleWritable> edge : this.getEdges()) {		
					context.sendMessage(edge.getDestVertexId(), messageOut);
				}
			}
		}
		
		@Override 
		public void cleanup(WorkerContext<IntWritable, ArrayWritable, DoubleWritable, ArrayWritable> context) throws IOException {
			context.write(this.getId(), this.getValue());
		}
	}
	
	public static class CFGraphLoader extends GraphLoader<IntWritable, ArrayWritable, DoubleWritable, ArrayWritable> {

		@Override
		public void load(LongWritable recordNumber, WritableRecord record,
				MutationContext<IntWritable, ArrayWritable, DoubleWritable, ArrayWritable> context) throws IOException {
			CFVertex uvertex = new CFVertex();
			uvertex.setValue(getDefaultArrayWritable());
			uvertex.setId((IntWritable)record.get(0));
			uvertex.addEdge((IntWritable)record.get(1), (DoubleWritable)record.get(2));
			context.addVertexRequest(uvertex);
			
			CFVertex mvertex = new CFVertex();
			mvertex.setValue(getDefaultArrayWritable());
			mvertex.setId((IntWritable)record.get(1));
			mvertex.addEdge((IntWritable)record.get(0), (DoubleWritable)record.get(2));
			context.addVertexRequest(mvertex);
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		
		if(args.length != 3) {
			System.out.println("Usage: <input table> <output table> <iterations>");
		}
		
		GraphJob job = new GraphJob();
		
		job.setGraphLoaderClass(CFGraphLoader.class);
		job.setVertexClass(CFVertex.class);
		job.addInput(TableInfo.builder().tableName(args[0]).build());
		job.addOutput(TableInfo.builder().tableName(args[1]).build());
		job.setMaxIteration(Integer.parseInt(args[2]));
		
		job.run();
	}
}