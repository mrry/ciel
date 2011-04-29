package skywriting.examples.skyhout.common;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortedPartitionedOutputCollector<K, V, C, R> extends
		AbstractOutputCollector<K, V, C, R> implements Closeable {

	private SequenceFile.Writer[] writers;
	
	private Combiner<K, C, V, R> combiner;
	
	public SortedPartitionedOutputCollector(SkywritingTaskFileSystem fs,
			Partitioner<K, V> partitioner, Combiner<K, C, V, R> combiner, Class<K> keyClass, Class<R> valueClass) throws IOException {
		this(fs, partitioner, combiner, keyClass, valueClass, fs.numOutputs());
			
	}
	
	
	public SortedPartitionedOutputCollector(SkywritingTaskFileSystem fs,
			Partitioner<K, V> partitioner, Combiner<K, C, V, R> combiner, Class<K> keyClass, Class<R> valueClass, int numOutputs) throws IOException {
		super(numOutputs, partitioner, combiner);
		
		this.combiner = combiner;
		this.writers = new SequenceFile.Writer[fs.numOutputs()];
		for (int i = 0; i < numOutputs; ++i) {
			this.maps.add(new TreeMap<K, C>());
			this.writers[i] = new SequenceFile.Writer(fs, fs.getConf(), new Path("/out/" + i), keyClass, valueClass);
		}
	}
	
	private void dump(int mapID) throws IOException {
	    Map<K, C> map = maps.get(mapID);
		Iterator<Map.Entry<K, C>> it = map.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<K, C> pairs = it.next();
	        writers[mapID].append(pairs.getKey(), this.combiner.combineFinal(pairs.getKey(), pairs.getValue()));
	    }
	}

	@Override
	public void close() throws IOException {
		for (int i = 0; i < this.numOutputs; i++) {
			Map<K, C> map = maps.get(i);
			dump(i);
			map.clear();
		}
		for (SequenceFile.Writer writer : this.writers) {
			writer.close();
		}
	}
	
}
