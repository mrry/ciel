import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Map.Entry;

public abstract class AbstractOutputCollector<K, V> implements OutputCollector<K, V>, Iterable<Map.Entry<K, V>> {

	protected ArrayList<Map<K, V>> maps;

	@Override
	public void collect(K key, V value) throws IOException {
		int nMaps = maps.size();
		
		// Work out which HashMap (partition) this word should go into
		int hc = key.hashCode();
		int targetMap = (hc < 0 ? -hc : hc) % nMaps;
		//System.out.println(key + " goes into map " + targetMap);
		Map<K, V> hmap = maps.get(targetMap);
		
		// Insert element into map
		if (hmap.containsKey(key)) {
			hmap.put(key, comb.combine(hmap.get(key), value));
		} else {
			hmap.put(key, comb.combineInit(value));
		}
	}

	protected Combiner<V> comb;

	public AbstractOutputCollector() {
		super();
	}

	public Iterator<Entry<K, V>> iterator() {
		// 
		return new MultiMapIterator();
	}

	class MultiMapIterator implements Iterator<Map.Entry<K, V>> {

		private int index;
		private Iterator<Map.Entry<K, V>> cur;
		
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
		public Entry<K, V> next() {
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