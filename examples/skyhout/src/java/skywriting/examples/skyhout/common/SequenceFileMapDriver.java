package skywriting.examples.skyhout.common;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SequenceFileMapDriver<K extends Writable, V extends Writable, K2, V2> extends MapDriver<K, V, K2, V2> {

	private SequenceFile.Reader reader;
	
	public SequenceFileMapDriver(SkywritingTaskFileSystem fs, ClosableOutputCollector<K2, V2> outputCollector, Mapper<K, V, K2, V2> mapper, Class<K> inputKeyClass, Class<V> inputValueClass) throws IOException {
		super(outputCollector, mapper, inputKeyClass, inputValueClass);
		this.reader = new SequenceFile.Reader(fs, new Path("/in/0"), fs.getConf());
	}
	
	public boolean readNextKeyValue(K key, V value) throws IOException {
		try {
			return this.reader.next(key, value);
		} catch (EOFException eofe) {
			this.reader.close();
			return false;
		}
		
	}
	
}
