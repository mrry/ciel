package skywriting.examples.skyhout.input;

import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SequenceFileReader implements Reader {

	private SequenceFile.Reader reader;
	private Class<? extends Writable> valueClass;
	
	public SequenceFileReader(SequenceFile.Reader reader) {
		this.reader = reader;
		this.valueClass = null;
	}

	@Override
	public boolean next(Writable key, Writable value) throws IOException {
		return reader.next(key, value);
	}
	
	protected void forgetFile() {
		this.reader = null;
	}

	@Override
	public Class<? extends Writable> getValueClass() {
		if (this.valueClass == null) {
			this.valueClass = this.reader.getValueClass().asSubclass(Writable.class);
		}
		return this.valueClass;
	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}
	
	
	
}
