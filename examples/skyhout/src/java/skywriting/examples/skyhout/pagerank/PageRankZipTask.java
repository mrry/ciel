package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import skywriting.examples.skyhout.common.ClosableOutputCollector;
import skywriting.examples.skyhout.common.IntArrayWritable;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;
import skywriting.examples.skyhout.common.ZipDriver;
import skywriting.examples.skyhout.common.Zipper;

public class PageRankZipTask extends SkyhoutTask {

	private static class PageRankZipper implements Zipper<IntWritable, IntArrayWritable, DoubleWritable, IntWritable, DoubleWritable> {

		@Override
		public void zip(IntWritable key, IntArrayWritable value1, DoubleWritable value2,
				OutputCollector<IntWritable, DoubleWritable> output) throws IOException {

			double distributedScore = value2.get() / (double) value1.get().length;
			for (int outLink : value1.get()) {
				output.collect(new IntWritable(outLink), new DoubleWritable(distributedScore));
			}
			
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {

		ClosableOutputCollector<IntWritable, DoubleWritable> output = new SortedPartitionedOutputCollector<IntWritable, DoubleWritable, DoubleWritable, DoubleWritable>(fs, new HashPartitioner<IntWritable, DoubleWritable>(), new PageRankCombinerReducer(), IntWritable.class, DoubleWritable.class);
		new ZipDriver<IntWritable, IntArrayWritable, DoubleWritable, IntWritable, DoubleWritable>(fs, new PageRankZipper(), output, IntWritable.class, IntArrayWritable.class, DoubleWritable.class).runZip();
		
	}

}
