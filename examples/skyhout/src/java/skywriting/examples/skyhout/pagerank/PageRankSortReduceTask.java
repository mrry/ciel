package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import skywriting.examples.skyhout.common.CombinerReducer;
import skywriting.examples.skyhout.common.IntArrayWritable;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedInputTextOutputReduceDriver;

public class PageRankSortReduceTask extends SkyhoutTask {

	private static class SetReducer implements CombinerReducer<DoubleWritable, IntArrayWritable, Set<Integer>, DoubleWritable, IntWritable> {

		@Override
		public void reduce(DoubleWritable key, Set<Integer> value,
				OutputCollector<DoubleWritable, IntWritable> output)
				throws IOException {
			for (int x : value) {
				output.collect(key, new IntWritable(x));
			}
		}

		@Override
		public Set<Integer> combine(Set<Integer> oldValue,
				IntArrayWritable newValue) {
			for (int x : newValue.get()) {
				oldValue.add(x);
			}
			return oldValue;
		}

		@Override
		public IntWritable combineFinal(DoubleWritable key,
				Set<Integer> oldValue) throws IOException {
			return null;
		}

		@Override
		public Set<Integer> combineInit(IntArrayWritable initVal) {
			Set<Integer> ret = new TreeSet<Integer>();
			for (int x : initVal.get()) {
				ret.add(x);
			}
			return ret;
		}



		
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		new SortedInputTextOutputReduceDriver<DoubleWritable, IntArrayWritable, Set<Integer>, DoubleWritable, IntWritable>(fs, new SetReducer(), DoubleWritable.class, IntArrayWritable.class, DoubleWritable.class, IntWritable.class).runReduce();
	}

}
