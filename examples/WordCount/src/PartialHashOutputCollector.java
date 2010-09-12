import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class PartialHashOutputCollector<K extends Writable, V extends Writable> implements OutputCollector<K, V> {

	ArrayList<HashMap<K, V>> maps;
	int flushThresh;
	DataOutputStream[] os;
	Combiner<V> comb;
	
	public PartialHashOutputCollector(DataOutputStream[] out, int numMaps, int flushThreshold, Combiner<V> combiner) {
		flushThresh = flushThreshold;
		os = out;
		comb = combiner;
		
		maps = new ArrayList<HashMap<K, V>>(numMaps);
		for (int i = 0; i < numMaps; i++) 
			maps.add(new HashMap<K, V>());
	}
	
	
	@Override
	public void collect(K key, V value) throws IOException {
		int nMaps = maps.size();
		
		// Work out which HashMap (partition) this word should go into
		int hc = key.hashCode();
		int targetMap = (hc < 0 ? -hc : hc) % nMaps;
		//System.out.println(key + " goes into map " + targetMap);
		HashMap<K, V> hmap = maps.get(targetMap);
		
		if (hmap.size() > flushThresh) {
			// Flush out the hashmap
			//System.err.println("flushing");
			flush(targetMap);
		}
		
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
		HashMap<K,V> hmap = maps.get(mapID);
		dump(mapID);
		
		// ... and clear it
		hmap.clear();
	}
	
	private void dump(int mapID) throws IOException {
	    HashMap<K, V> map = maps.get(mapID);
		Iterator<Map.Entry<K, V>> it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<K, V> pairs = it.next();
	        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        
	        // Write to output stream
	        pairs.getKey().write(os[mapID]);
	        pairs.getValue().write(os[mapID]);
	    }
	}
	
}
