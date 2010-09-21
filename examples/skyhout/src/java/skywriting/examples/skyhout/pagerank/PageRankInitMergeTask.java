package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import skywriting.examples.skyhout.common.CombinerReducer;
import skywriting.examples.skyhout.common.IntArrayWritable;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedInputReduceDriver;

public class PageRankInitMergeTask extends SkyhoutTask {

	private static class PageRankAdjacencyListReducer implements CombinerReducer<IntWritable, IntArrayWritable, List<Integer>, IntWritable, IntArrayWritable> {

		@Override
		public void reduce(IntWritable key, List<Integer> value,
				OutputCollector<IntWritable, IntArrayWritable> output)
				throws IOException {
			output.collect(key, this.combineFinal(key, value));
		}

		@Override
		public List<Integer> combine(List<Integer> oldValue,
				IntArrayWritable newValue) {
			for (int x : newValue.get()) {
				oldValue.add(x);
			}
			return oldValue;
		}

		@Override
		public IntArrayWritable combineFinal(IntWritable key,
				List<Integer> oldValue) throws IOException {
			int[] arr = new int[oldValue.size()];
			int i = 0;
			for (int x : arr) {
				arr[i++] = x;
			}
			return new IntArrayWritable(arr);
		}

		@Override
		public List<Integer> combineInit(IntArrayWritable initVal) {
			List<Integer> ret = new LinkedList<Integer>();
			int[] arr = initVal.get();
			for (int x : arr) {
				ret.add(x);
			}
			return ret;
		}
		
	
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		
		new SortedInputReduceDriver<IntWritable, IntArrayWritable, List<Integer>, IntWritable, IntArrayWritable>(fs,
				new PageRankAdjacencyListReducer(),
				IntWritable.class, IntArrayWritable.class, IntWritable.class, IntArrayWritable.class);
		
	}

}
