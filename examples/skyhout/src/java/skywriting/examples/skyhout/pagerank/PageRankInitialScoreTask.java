package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import skywriting.examples.skyhout.common.ClosableOutputCollector;
import skywriting.examples.skyhout.common.DirectOutputCollector;
import skywriting.examples.skyhout.common.IntArrayWritable;
import skywriting.examples.skyhout.common.Mapper;
import skywriting.examples.skyhout.common.SequenceFileMapDriver;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;

public class PageRankInitialScoreTask extends SkyhoutTask {

	private static class PageRankInitialScoreMapper implements Mapper<IntWritable, IntArrayWritable, IntWritable, DoubleWritable> {

		@Override
		public void map(IntWritable key, IntArrayWritable value,
				OutputCollector<IntWritable, DoubleWritable> output) throws IOException {
			output.collect(new IntWritable(key.get()), new DoubleWritable(1.0));
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		ClosableOutputCollector<IntWritable, DoubleWritable> output = new DirectOutputCollector<IntWritable, DoubleWritable>(fs, IntWritable.class, DoubleWritable.class);
		new SequenceFileMapDriver<IntWritable, IntArrayWritable, IntWritable, DoubleWritable>(fs, output, new PageRankInitialScoreMapper(), IntWritable.class, IntArrayWritable.class).runMap();
	}

}
