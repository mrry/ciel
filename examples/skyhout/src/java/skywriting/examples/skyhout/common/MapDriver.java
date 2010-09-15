package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

public abstract class MapDriver<K extends Writable, V extends Writable, K2, V2> {

	private Mapper<K, V, K2, V2> mapper;
	private ClosableOutputCollector<K2, V2> outputCollector;

	private K currentKey;
	private V currentValue;
	
	public MapDriver(ClosableOutputCollector<K2, V2> outputCollector, Mapper<K, V, K2, V2> mapper, Class<K> inputKeyClass, Class<V> inputValueClass) throws IOException {
		this.outputCollector = outputCollector;
		this.mapper = mapper;
		try {
			this.currentKey = inputKeyClass.newInstance();
			this.currentValue = inputValueClass.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	
	public void runMap() throws IOException {
		while (this.readNextKeyValue(this.currentKey, this.currentValue))
			this.mapper.map(currentKey, currentValue, this.outputCollector);
		this.outputCollector.close();
	}
	
	public abstract boolean readNextKeyValue(K key, V value) throws IOException;
	
}
