package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;

public interface Mapper<K, V, K2, V2> {

	void map(K key, V value, OutputCollector<K2, V2> output) throws IOException;
	
}
