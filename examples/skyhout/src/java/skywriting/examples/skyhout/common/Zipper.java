package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.mapred.OutputCollector;

public interface Zipper<K, V1, V2, KOUT, VOUT> {

	void zip(K key, V1 value1, V2 value2, OutputCollector<KOUT, VOUT> output) throws IOException;
	
}
