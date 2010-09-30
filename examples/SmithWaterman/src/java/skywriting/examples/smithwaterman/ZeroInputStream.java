package skywriting.examples.smithwaterman;

import java.io.IOException;
import java.io.InputStream;

public class ZeroInputStream extends InputStream {

	@Override
	public int read() throws IOException {
		return 0;
	}

}
