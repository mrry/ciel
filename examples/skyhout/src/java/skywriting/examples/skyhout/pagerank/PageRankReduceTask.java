package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedInputReduceDriver;

public class PageRankReduceTask extends SkyhoutTask {

	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		
		SortedInputReduceDriver<Text, DoubleWritable, Text, DoubleWritable> reducer = 
			new SortedInputReduceDriver<Text, DoubleWritable, Text, DoubleWritable>(fs, new PageRankCombinerReducer(), Text.class, DoubleWritable.class, Text.class, DoubleWritable.class);
	
		reducer.runReduce();
		
	}

}
