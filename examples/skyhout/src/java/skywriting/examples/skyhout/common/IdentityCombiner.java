package skywriting.examples.skyhout.common;

class IdentityCombiner<T> implements Combiner<T, T> {
	
	public T combine(T oldValue, T value) {
		return value;
	}
	
	
	public T combineInit(T initVal) {
		return initVal;
	}
	
}
