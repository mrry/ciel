package skywriting.examples.skyhout.common;

import java.io.IOException;

public interface Combiner<K, C, V, R> {
	
	public C combine(C oldValue, V newValue);
	
	public C combineInit(V initVal);
	
	public R combineFinal(K key, C oldValue) throws IOException;
	
}
	