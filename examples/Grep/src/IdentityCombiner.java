
class IdentityCombiner<T> implements Combiner<T> {
	
	public T combine(T oldValue, T value) {
		
		return value;
		
	}
	
}
