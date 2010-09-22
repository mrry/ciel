package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import skywriting.examples.skyhout.common.Combiner;
import skywriting.examples.skyhout.common.CombinerReducer;
import skywriting.examples.skyhout.common.IntArrayWritable;

public class PageRankCombinerReducer<K> implements CombinerReducer<K, DoubleWritable, DoubleWritable, K, DoubleWritable> {

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
	public void reduce(K key, DoubleWritable value,
			OutputCollector<K, DoubleWritable> output) throws IOException {
		output.collect(key, value);
	}

	@Override
	public DoubleWritable combineFinal(K key, DoubleWritable oldValue)
			throws IOException {
		return oldValue;
	}
	
}
