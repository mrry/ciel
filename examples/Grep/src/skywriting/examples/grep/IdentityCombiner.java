package skywriting.examples.grep;

class IdentityCombiner<T> implements Combiner<T> {
	
	public T combine(T oldValue, T value) {
		
		return value;
		
	}
	
	
	public T combineInit(T initVal) {
		
		return initVal;
		
	}
	
}
