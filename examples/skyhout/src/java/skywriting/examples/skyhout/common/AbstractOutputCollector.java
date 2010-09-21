package skywriting.examples.skyhout.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.mapreduce.Partitioner;

public abstract class AbstractOutputCollector<K, V, C, R> implements ClosableOutputCollector<K, V> /*, Iterable<Map.Entry<K, R>>*/ {

	protected final int numOutputs;
	protected final ArrayList<Map<K, C>> maps;
	private final Partitioner<K, V> partitioner;
	private final Combiner<K, C, V, R> comb;
	
	public AbstractOutputCollector(int numOutputs, Partitioner<K, V> partitioner, Combiner<K, C, V, R> combiner) {
		System.err.println("New AOC with " + numOutputs + " outputs");
		this.numOutputs = numOutputs;
		this.partitioner = partitioner;
		this.comb = combiner;
		this.maps = new ArrayList<Map<K, C>>(numOutputs);
	}

	public abstract void close() throws IOException;
	
	@Override
	public void collect(K key, V value) throws IOException {
		this.collectWithIndex(key, value);
	}
	
	public int collectWithIndex(K key, V value) throws IOException {
		int partitionIndex = this.partitioner.getPartition(key, value, this.numOutputs);
		System.err.println("Partition index: " + partitionIndex);
		Map<K, C> map = maps.get(partitionIndex);
		
		// Insert element into map
		if (map.containsKey(key)) {
			map.put(key, comb.combine(map.get(key), value));
		} else {
			map.put(key, comb.combineInit(value));
		}
		
		return partitionIndex;
	}
/*
	public Iterator<Entry<K, R>> iterator() {
		// 
		return new MultiMapIterator();
	}

	class MultiMapIterator implements Iterator<Map.Entry<K, R>> {

		private int index;
		private Iterator<Map.Entry<K, C>> cur;
		
		public MultiMapIterator() {
			index = 0;
			cur = null;
		}
		
		@Override
		public boolean hasNext() {
			// 
			boolean ret;
			if (cur == null) {
				ret = nextMap();
				if (!ret) return false;
			}
			
			ret = cur.hasNext();
			
			if (!ret) {
				cur = null;
				return hasNext();
			}
			
			return ret;
		}

		@Override
		public Entry<K, R> next() {
			// 
			if (!hasNext()) throw new NoSuchElementException();
			else return AbstractOutputCollector.this.comb.combineFinal(cur.next());
		}

		@Override
		public void remove() {
			// 
			throw new UnsupportedOperationException();
		}
		
		private boolean nextMap() {
			if (index < maps.size()) {
				cur = maps.get(index++).entrySet().iterator();
				return true;
			} else
				return false;
		}
	}
*/
	
}