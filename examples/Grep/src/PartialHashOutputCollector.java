import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class PartialHashOutputCollector<K extends Writable, V extends Writable> extends AbstractOutputCollector<K, V> {

	int flushThresh;
	DataOutputStream[] os;
	public PartialHashOutputCollector(DataOutputStream[] out, int numMaps, int flushThreshold, Combiner<V> combiner) {
		flushThresh = flushThreshold;
		os = out;
		comb = combiner;
		
		maps = new ArrayList<Map<K, V>>(numMaps);
		for (int i = 0; i < numMaps; i++) 
			maps.add(new HashMap<K, V>());
	}
	
	
	@Override
	public void collect(K key, V value) throws IOException {
		super.collect(key, value);
		
		int nMaps = maps.size();
		// Work out which HashMap (partition) this word should go into
		int hc = key.hashCode();
		int targetMap = (hc < 0 ? -hc : hc) % nMaps;

		if (maps.get(targetMap).size() > flushThresh) {
			// Flush out the hashmap
			//System.err.println("flushing");
			flush(targetMap);
		}
		
	}
	
	
	public void flushAll() throws IOException {
		for (int i = 0; i < maps.size(); i++) 
			flush(i);
	}
	

	private void flush(int mapID) throws IOException {
		// Flush out the hashmap
		Map<K,V> hmap = maps.get(mapID);
		dump(mapID);
		
		// ... and clear it
		hmap.clear();
	}
	
	private void dump(int mapID) throws IOException {
	    Map<K, V> map = maps.get(mapID);
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
