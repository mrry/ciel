package skywriting.examples.skyhout.common;

class IdentityCombiner<K, T> implements Combiner<K, T, T, T> {
	
	public T combine(T oldValue, T value) {
		return value;
	}
	
	
	public T combineInit(T initVal) {
		return initVal;
	}
	
	public T combineFinal(K key, T oldVal) {
		return oldVal;
	}
	
}
