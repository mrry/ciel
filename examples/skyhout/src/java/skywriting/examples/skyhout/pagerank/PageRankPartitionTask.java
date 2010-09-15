package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.hadoop.io.ArrayWritable;
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
import skywriting.examples.skyhout.common.StringArrayWritable;

public class PageRankPartitionTask extends SkyhoutTask {

	private static class PageRankParserMapper implements Mapper<LongWritable, Text, Text, StringArrayWritable> {

		private static final Pattern SPLIT_PATTERN = Pattern.compile("\t");
		
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, StringArrayWritable> output) throws IOException {
			String[] splitLine = SPLIT_PATTERN.split(value.toString());
			String[] outLinks = Arrays.copyOfRange(splitLine, 1, splitLine.length);
			System.out.println("Mapping key: " + splitLine[0]);
			output.collect(new Text(splitLine[0]), new StringArrayWritable(outLinks));
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		ClosableOutputCollector<Text, StringArrayWritable> output = new SortedPartitionedOutputCollector<Text, StringArrayWritable, StringArrayWritable>(fs, new HashPartitioner<Text, StringArrayWritable>(), new NullCombiner<Text, StringArrayWritable>(), Text.class, StringArrayWritable.class);
		new LineRecordFileMapDriver<Text, StringArrayWritable>(fs, output, new PageRankParserMapper()).runMap();
	}

}
