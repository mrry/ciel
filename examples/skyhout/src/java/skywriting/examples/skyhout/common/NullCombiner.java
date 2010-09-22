package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;


public class NullCombiner<K, V> implements CombinerReducer<K, V, V, K, V> {

	@Override
	public V combine(V oldValue, V newValue) {

		throw new UnsupportedOperationException("We assume that all keys are unique");
	}

	@Override
	public V combineInit(V initVal) {
		return initVal;
	}

	public V combineFinal(K key, V oldVal) {
		return oldVal;
	}
	
	@Override
	public void reduce(K key, V value, OutputCollector<K, V> output)
			throws IOException {
		output.collect(key, this.combineFinal(key, value));
	}

}
