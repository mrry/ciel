package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;

public interface CombinerReducer<K, V, K2, V2> extends Combiner<V, V> {
	
	public void reduce(K key, V value, OutputCollector<K2, V2> output) throws IOException;
	
}