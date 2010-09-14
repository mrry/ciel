package skyhout.common;

import java.util.Set;
import java.util.TreeSet;


class SetInsertCombiner<T> implements Combiner<Set<T>> {
	
	public Set<T> combine(Set<T> oldValue, Set<T> increment) {
		oldValue.addAll(increment);
			
		return oldValue;
	}
	
	
	public Set<T> combineInit(Set<T> initVal) {
		
		return new TreeSet<T>(initVal);
		
	}
	
}
