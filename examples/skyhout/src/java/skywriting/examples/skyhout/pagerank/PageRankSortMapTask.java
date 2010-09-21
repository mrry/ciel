package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import skywriting.examples.skyhout.common.ClosableOutputCollector;
import skywriting.examples.skyhout.common.Combiner;
import skywriting.examples.skyhout.common.IntArrayWritable;
import skywriting.examples.skyhout.common.Mapper;
import skywriting.examples.skyhout.common.SequenceFileMapDriver;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;

public class PageRankSortMapTask extends SkyhoutTask {

	private static class InverseMapper implements Mapper<IntWritable, DoubleWritable, DoubleWritable, IntWritable> {

		@Override
		public void map(IntWritable key, DoubleWritable value,
				OutputCollector<DoubleWritable, IntWritable> output)
				throws IOException {
			output.collect(new DoubleWritable(value.get()), key);
		}


	}
	
	private static class SetCombiner implements Combiner<DoubleWritable, Set<Integer>, IntWritable, IntArrayWritable> {

		@Override
		public Set<Integer> combine(Set<Integer> oldValue, IntWritable newValue) {
			oldValue.add(newValue.get());
			return oldValue;
		}

		@Override
		public IntArrayWritable combineFinal(DoubleWritable key,
				Set<Integer> oldValue) throws IOException {
			int[] arr = new int[oldValue.size()];
			int i = 0;
			for (int x : oldValue) {
				arr[i++] = x;
			}
			return new IntArrayWritable(arr);
		}

		@Override
		public Set<Integer> combineInit(IntWritable initVal) {
			Set<Integer> ret = new TreeSet<Integer>();
			ret.add(initVal.get());
			return ret;
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		
		ClosableOutputCollector<DoubleWritable, IntWritable> output = new SortedPartitionedOutputCollector<DoubleWritable, IntWritable, Set<Integer>, IntArrayWritable>(fs, new HashPartitioner<DoubleWritable, IntWritable>(), new SetCombiner(), DoubleWritable.class, IntArrayWritable.class);
		new SequenceFileMapDriver<IntWritable, DoubleWritable, DoubleWritable, IntWritable>(fs, output, new InverseMapper(), IntWritable.class, DoubleWritable.class).runMap();
		
	}

}
