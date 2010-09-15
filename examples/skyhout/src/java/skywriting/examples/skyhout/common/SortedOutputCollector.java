package skywriting.examples.skyhout.common;

import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class SortedOutputCollector<K, V, C> extends AbstractOutputCollector<K, V, C> {

	TreeMap<K, C> sortedMap;
	
	public SortedOutputCollector(Combiner<C, V> combiner) {
		super(1, new HashPartitioner<K, V>(), combiner);
		sortedMap = new TreeMap<K, C>();
		this.maps.add(sortedMap);
	}
	
	Map<K, C> descendingMap() {
		return sortedMap.descendingMap();
	}
		
	public void close() {
		;
	}
	
}
