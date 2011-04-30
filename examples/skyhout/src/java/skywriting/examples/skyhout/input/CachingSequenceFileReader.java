package skywriting.examples.skyhout.input;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class CachingSequenceFileReader extends SequenceFileReader {
	
	private final LinkedList<Writable> keys;
	private final LinkedList<Writable> values;
	
	public CachingSequenceFileReader(SequenceFile.Reader reader) {
		super(reader);
		this.keys = new LinkedList<Writable>();
		this.values = new LinkedList<Writable>();
	}
	
	public boolean next(Writable key, Writable value) throws IOException {
		boolean ret = super.next(key, value);
		this.keys.addLast(key);
		this.values.addLast(value);
		return ret;
	}
	
	public void cache() {
		this.forgetFile();
	}
	
	public Iterator<Writable> getKeyIterator() {
		return this.keys.iterator();
	}
	
	public Iterator<Writable> getValueIterator() {
		return this.values.iterator();
	}
	
	
}
