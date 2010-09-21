package skywriting.examples.skyhout.common;

import java.util.Set;
import java.util.TreeSet;


class SetInsertCombiner<K, T> implements Combiner<K, Set<T>, T, Set<T>> {
	
	public Set<T> combine(Set<T> oldValue, T increment) {
		oldValue.add(increment);
		return oldValue;
	}
	
	
	public Set<T> combineInit(T initVal) {
		TreeSet<T> ret = new TreeSet<T>();
		ret.add(initVal);
		return ret;
	}

	@Override
	public Set<T> combineFinal(K key, Set<T> oldValue) {
		return oldValue;
	}

}
