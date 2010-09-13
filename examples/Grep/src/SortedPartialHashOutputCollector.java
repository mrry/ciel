import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Map;

class SortedPartialHashOutputCollector<K extends Writable, V extends Writable> implements OutputCollector<K, V> {

	ArrayList<TreeMap<K, V>> maps;
	DataOutputStream[] os;
	Combiner<V> comb;
	
	public SortedPartialHashOutputCollector(DataOutputStream[] out, int numMaps, Combiner<V> combiner) {
		os = out;
		comb = combiner;
		
		maps = new ArrayList<TreeMap<K, V>>(numMaps);
		for (int i = 0; i < numMaps; i++) 
			maps.add(new TreeMap<K, V>());
	}
	
	
	@Override
	public void collect(K key, V value) throws IOException {
		int nMaps = maps.size();
		
		// Work out which HashMap (partition) this word should go into
		int hc = key.hashCode();
		int targetMap = (hc < 0 ? -hc : hc) % nMaps;
		//System.out.println(key + " goes into map " + targetMap);
		TreeMap<K, V> hmap = maps.get(targetMap);
		
		// Insert element into map
		if (hmap.containsKey(key)) {
			hmap.put(key, comb.combine(hmap.get(key), value));
		} else {
			hmap.put(key, value);
		}
	}
	
	
	public void flushAll() throws IOException {
		for (int i = 0; i < maps.size(); i++) 
			flush(i);
	}
	
	
	private void flush(int mapID) throws IOException {
		// Flush out the hashmap
		TreeMap<K,V> hmap = maps.get(mapID);
		dump(mapID);
		
		// ... and clear it
		hmap.clear();
	}
	
	private void dump(int mapID) throws IOException {
	    TreeMap<K, V> map = maps.get(mapID);
		Iterator<Map.Entry<K, V>> it = map.descendingMap().entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<K, V> pairs = it.next();
	        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        
	        // Write to output stream
	        pairs.getKey().write(os[mapID]);
	        pairs.getValue().write(os[mapID]);
	    }
	}
	
}
