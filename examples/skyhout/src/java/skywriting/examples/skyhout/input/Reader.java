package skywriting.examples.skyhout.input;

import java.io.IOException;

import org.apache.hadoop.io.Writable;

public interface Reader {

	public boolean next(Writable key, Writable value) throws IOException;
	
	public Class<? extends Writable> getValueClass();
	
	public void close() throws IOException;
	
}
