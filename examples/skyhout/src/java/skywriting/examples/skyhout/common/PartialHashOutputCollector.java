package skywriting.examples.skyhout.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class PartialHashOutputCollector<K extends Writable, V extends Writable> extends AbstractOutputCollector<K, V> {

	private int flushThresh;
	private SequenceFile.Writer[] writers;
	
	public PartialHashOutputCollector(SkywritingTaskFileSystem fs, Configuration conf, Class<K> keyClass, Class<V> valueClass, int flushThreshold, Combiner<V> combiner) throws IOException {
		this.flushThresh = flushThreshold;
		this.comb = combiner;
		
		int numOutputs = fs.numOutputs();
		maps = new ArrayList<Map<K, V>>(numOutputs);
		writers = new SequenceFile.Writer[numOutputs];
		for (int i = 0; i < numOutputs; i++) { 
			maps.add(new HashMap<K, V>());
			writers[i] = new SequenceFile.Writer(fs, conf, new Path("/out/" + i), keyClass, valueClass);
		}
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
	
	public void closeWriters() throws IOException {
		for (SequenceFile.Writer w : this.writers) {
			w.close();
		}
	}
	
	private void dump(int mapID) throws IOException {
	    Map<K, V> map = maps.get(mapID);
		Iterator<Map.Entry<K, V>> it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<K, V> pairs = it.next();
	        //System.out.println(pairs.getKey() + " = " + pairs.getValue());
	        
	        // Write to output stream
	        writers[mapID].append(pairs.getKey(), pairs.getValue());
	    }
	}

}
