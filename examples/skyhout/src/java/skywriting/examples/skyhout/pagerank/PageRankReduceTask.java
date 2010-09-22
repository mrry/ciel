package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedInputReduceDriver;
import skywriting.examples.skyhout.common.SortedInputSequenceFileOutputReduceDriver;

public class PageRankReduceTask extends SkyhoutTask {

	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		
		SortedInputReduceDriver<IntWritable, DoubleWritable, DoubleWritable, IntWritable, DoubleWritable> reducer = 
			new SortedInputSequenceFileOutputReduceDriver<IntWritable, DoubleWritable, DoubleWritable, IntWritable, DoubleWritable>(fs, new PageRankCombinerReducer<IntWritable>(), IntWritable.class, DoubleWritable.class, IntWritable.class, DoubleWritable.class);
	
		reducer.runReduce();
		
	}

}
