package skywriting.examples.skyhout.common;

import java.util.Set;
import java.util.TreeSet;


class SetInsertCombiner<T> implements Combiner<Set<T>, T> {
	
	public Set<T> combine(Set<T> oldValue, T increment) {
		oldValue.add(increment);
		return oldValue;
	}
	
	
	public Set<T> combineInit(T initVal) {
		TreeSet<T> ret = new TreeSet<T>();
		ret.add(initVal);
		return ret;
	}
	
}
