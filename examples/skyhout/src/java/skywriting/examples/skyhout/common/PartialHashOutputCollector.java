package skywriting.examples.skyhout.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class PartialHashOutputCollector<K extends Writable, V, C extends Writable> extends AbstractOutputCollector<K, V, C> {

	private int flushThresh;
	private SequenceFile.Writer[] writers;
	private int numOutputs;
	
	public PartialHashOutputCollector(SkywritingTaskFileSystem fs, Configuration conf, Class<K> keyClass, Class<C> valueClass, int flushThreshold, Combiner<C, V> combiner) throws IOException {
		this(fs, conf, keyClass, valueClass, flushThreshold, combiner, new HashPartitioner<K, V>());
	}
	
	public PartialHashOutputCollector(SkywritingTaskFileSystem fs, Configuration conf, Class<K> keyClass, Class<C> valueClass, int flushThreshold, Combiner<C, V> combiner, Partitioner<K,V> partitioner) throws IOException {
		super(fs.numOutputs(), partitioner, combiner);
		
		this.flushThresh = flushThreshold;
		
		this.writers = new SequenceFile.Writer[numOutputs];
		for (int i = 0; i < numOutputs; i++) { 
			this.maps.add(new HashMap<K, C>());
			this.writers[i] = new SequenceFile.Writer(fs, conf, new Path("/out/" + i), keyClass, valueClass);
		}
	}
	
	@Override
	public void collect(K key, V value) throws IOException {
		int mapIndex = super.collectWithIndex(key, value);
		
		if (this.maps.get(mapIndex).size() > flushThresh) {
			flush(mapIndex);
		}
	}
	
	public void close() throws IOException {
		this.flushAll();
		this.closeWriters();
	}
	
	public void flushAll() throws IOException {
		for (int i = 0; i < this.numOutputs; i++) 
			flush(i);
	}
	

	private void flush(int mapID) throws IOException {
		// Flush out the hashmap
		Map<K, C> hmap = maps.get(mapID);
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
	    Map<K, C> map = maps.get(mapID);
		Iterator<Map.Entry<K, C>> it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<K, C> pairs = it.next();
	        writers[mapID].append(pairs.getKey(), pairs.getValue());
	    }
	}

}
