package skywriting.examples.grep;
public interface Combiner<V> {
	
	public V combine(V oldValue, V newValue);
	
	public V combineInit(V initVal);
	
}
	