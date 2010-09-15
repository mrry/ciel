package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import skywriting.examples.skyhout.common.NullCombiner;
import skywriting.examples.skyhout.common.SkyhoutTask;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedInputReduceDriver;

public class PageRankInitMergeTask extends SkyhoutTask {

	@Override
	public void invoke(SkywritingTaskFileSystem fs, String[] args)
			throws IOException {
		new SortedInputReduceDriver<Text, DoubleWritable, Text, DoubleWritable>(fs, new NullCombiner<Text, DoubleWritable>(), Text.class, DoubleWritable.class, Text.class, DoubleWritable.class).runReduce();
	}

}
