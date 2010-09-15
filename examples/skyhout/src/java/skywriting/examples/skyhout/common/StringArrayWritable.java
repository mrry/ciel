package skywriting.examples.skyhout.common;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.UTF8;

public class StringArrayWritable extends ArrayWritable {

	public StringArrayWritable(String[] arg0) {
		super(arg0);
	}

	@SuppressWarnings("deprecation")
	public StringArrayWritable() {
		super(UTF8.class);
	}
	
}
