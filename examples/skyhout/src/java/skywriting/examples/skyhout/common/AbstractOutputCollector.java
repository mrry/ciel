package skywriting.examples.skyhout.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

import org.apache.hadoop.mapreduce.Partitioner;

public abstract class AbstractOutputCollector<K, V, C> implements ClosableOutputCollector<K, V>, Iterable<Map.Entry<K, C>> {

	protected final int numOutputs;
	protected final ArrayList<Map<K, C>> maps;
	private final Partitioner<K, V> partitioner;
	private final Combiner<C, V> comb;
	
	public AbstractOutputCollector(int numOutputs, Partitioner<K, V> partitioner, Combiner<C, V> combiner) {
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
		Map<K, C> map = maps.get(partitionIndex);
		
		// Insert element into map
		if (map.containsKey(key)) {
			map.put(key, comb.combine(map.get(key), value));
		} else {
			map.put(key, comb.combineInit(value));
		}
		
		return partitionIndex;
	}

	public Iterator<Entry<K, C>> iterator() {
		// 
		return new MultiMapIterator();
	}

	class MultiMapIterator implements Iterator<Map.Entry<K, C>> {

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
		public Entry<K, C> next() {
			// 
			if (!hasNext()) throw new NoSuchElementException();
			else return cur.next();
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

	
}