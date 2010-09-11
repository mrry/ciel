public interface Combiner<V> {
	
	public V combine(V oldValue, V newValue);
	
}
	