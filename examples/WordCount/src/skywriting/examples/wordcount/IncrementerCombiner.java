package skywriting.examples.wordcount;

class IncrementerCombiner implements Combiner<IntWritable> {
	
	public IntWritable combine(IntWritable oldValue, IntWritable increment) {
		IntWritable newValue = new IntWritable();
		
		newValue.set(oldValue.get() + increment.get());
		
		return newValue;
	}
	
}
