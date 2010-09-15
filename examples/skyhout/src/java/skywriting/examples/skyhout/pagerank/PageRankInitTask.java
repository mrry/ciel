package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import skywriting.examples.skyhout.common.ClosableOutputCollector;
import skywriting.examples.skyhout.common.LineRecordFileMapDriver;
import skywriting.examples.skyhout.common.Mapper;
import skywriting.examples.skyhout.common.NullCombiner;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;

public class PageRankInitTask extends SkyhoutTask {

	private static class PageRankInitMapper implements Mapper<LongWritable, Text, Text, DoubleWritable> {

		private static final Pattern SPLIT_PATTERN = Pattern.compile("\t");
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, DoubleWritable> output) throws IOException {
			String[] splitLine = SPLIT_PATTERN.split(value.toString());
			output.collect(new Text(splitLine[0]), new DoubleWritable(1.0));
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		ClosableOutputCollector<Text, DoubleWritable> output = new SortedPartitionedOutputCollector<Text, DoubleWritable, DoubleWritable>(fs, new HashPartitioner<Text, DoubleWritable>(), new NullCombiner<Text, DoubleWritable>(), Text.class, DoubleWritable.class);
		new LineRecordFileMapDriver<Text, DoubleWritable>(fs, output, new PageRankInitMapper()).runMap();
	}

}
