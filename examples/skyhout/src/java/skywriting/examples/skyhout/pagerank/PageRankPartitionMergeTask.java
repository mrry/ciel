package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import skywriting.examples.skyhout.common.NullCombiner;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedInputReduceDriver;
import skywriting.examples.skyhout.common.StringArrayWritable;

public class PageRankPartitionMergeTask extends SkyhoutTask {

	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		new SortedInputReduceDriver<Text, StringArrayWritable, Text, StringArrayWritable>(fs, new NullCombiner<Text, StringArrayWritable>(), Text.class, StringArrayWritable.class, Text.class, StringArrayWritable.class).runReduce();
	}

}
