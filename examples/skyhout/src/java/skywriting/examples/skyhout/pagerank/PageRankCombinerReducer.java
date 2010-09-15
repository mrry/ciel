package skywriting.examples.skyhout.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import skywriting.examples.skyhout.common.CombinerReducer;

public class PageRankCombinerReducer implements CombinerReducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public DoubleWritable combineInit(DoubleWritable initVal) {
		return new DoubleWritable(initVal.get());
	}

	@Override
	public DoubleWritable combine(DoubleWritable oldValue,
			DoubleWritable newValue) {
		oldValue.set(oldValue.get() + newValue.get());
		return oldValue;
	}

	@Override
	public void reduce(Text key, DoubleWritable value,
			OutputCollector<Text, DoubleWritable> output) throws IOException {
		output.collect(key, value);
	}
	
}
