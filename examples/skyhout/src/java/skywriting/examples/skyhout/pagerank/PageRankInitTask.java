package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import skywriting.examples.skyhout.common.ClosableOutputCollector;
import skywriting.examples.skyhout.common.IntArrayWritable;
import skywriting.examples.skyhout.common.LineRecordFileMapDriver;
import skywriting.examples.skyhout.common.Mapper;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;

/**
 * Class that converts [from] [to] lines into combined [from] [array of to] SequenceFiles.
 */
public class PageRankInitTask extends SkyhoutTask {

	private static class PageRankInitMapper implements Mapper<LongWritable, Text, IntWritable, IntWritable> {

		private static final Pattern SPLIT_PATTERN = Pattern.compile("\t");
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<IntWritable, IntWritable> output) throws IOException {
			String[] splitLine = SPLIT_PATTERN.split(value.toString());
			output.collect(new IntWritable(Integer.parseInt(splitLine[0])), new IntWritable(Integer.parseInt(splitLine[1])));
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		ClosableOutputCollector<IntWritable, IntWritable> output = 
			new SortedPartitionedOutputCollector<IntWritable, IntWritable, List<Integer>, IntArrayWritable>(fs,
					new HashPartitioner<IntWritable, IntWritable>(), 
					new IntListCombiner(), IntWritable.class, IntArrayWritable.class);
		
		new LineRecordFileMapDriver<IntWritable, IntWritable>(fs, output, new PageRankInitMapper()).runMap();
	}

}
