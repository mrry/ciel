package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;

public interface CombinerReducer<K, V, C, K2, V2> extends Combiner<K, C, V, V2> {
	
	public void reduce(K key, C value, OutputCollector<K2, V2> output) throws IOException;
	
}