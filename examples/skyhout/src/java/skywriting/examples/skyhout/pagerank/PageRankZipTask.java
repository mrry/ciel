package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import skywriting.examples.skyhout.common.ClosableOutputCollector;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;
import skywriting.examples.skyhout.common.StringArrayWritable;
import skywriting.examples.skyhout.common.ZipDriver;
import skywriting.examples.skyhout.common.Zipper;

public class PageRankZipTask extends SkyhoutTask {

	private static class PageRankZipper implements Zipper<Text, StringArrayWritable, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void zip(Text key, StringArrayWritable value1, DoubleWritable value2,
				OutputCollector<Text, DoubleWritable> output) throws IOException {
			String[] outLinks = value1.toStrings();
			DoubleWritable distributedScore = new DoubleWritable(value2.get() / (double) outLinks.length);
			for (String outLink : outLinks) {
				output.collect(new Text(outLink), distributedScore);
			}
		}
		
	}
	
	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {

		ClosableOutputCollector<Text, DoubleWritable> output = new SortedPartitionedOutputCollector<Text, DoubleWritable, DoubleWritable>(fs, new HashPartitioner<Text, DoubleWritable>(), new PageRankCombinerReducer(), Text.class, DoubleWritable.class);
		new ZipDriver<Text, StringArrayWritable, DoubleWritable, Text, DoubleWritable>(fs, new PageRankZipper(), output, Text.class, StringArrayWritable.class, DoubleWritable.class).runZip();
		
	}

}
