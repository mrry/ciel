package skywriting.examples.skyhout.common;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class SequenceFileUtils {

	public static boolean read(SequenceFile.Reader reader, Writable key, Writable value) throws IOException {
		try {
			return reader.next(key, value);
		} catch (EOFException eofe) {
			return false;
		}
	}
	
}
