package skywriting.examples.skyhout.common;

import java.io.Closeable;

import org.apache.hadoop.mapred.OutputCollector;

public interface ClosableOutputCollector<K, V> extends OutputCollector<K, V>, Closeable {

}
