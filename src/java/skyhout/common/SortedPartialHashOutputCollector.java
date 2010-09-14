package skyhout.common;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map;

public class SortedPartialHashOutputCollector<K, V> extends AbstractOutputCollector<K,V> {

	TreeMap<K,V> sortedMap;
	
	public SortedPartialHashOutputCollector(Combiner<V> combiner) {
		comb = combiner;
	
		sortedMap = new TreeMap<K, V>();
		
		maps = new ArrayList<Map<K, V>>(1);
		maps.add(sortedMap);
	}
	
	Map<K,V> descendingMap() {
		return sortedMap.descendingMap();
	}
		
}
