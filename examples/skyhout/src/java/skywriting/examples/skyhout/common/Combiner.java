package skywriting.examples.skyhout.common;

public interface Combiner<C, V> {
	
	public C combine(C oldValue, V newValue);
	
	public C combineInit(V initVal);
	
}
	