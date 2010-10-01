package skywriting.examples.skyhout.common;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

public class FakeSeekable extends InputStream implements Seekable, PositionedReadable {

	private InputStream stream;
	private long pos;
	
	public FakeSeekable(InputStream stream) {
		this.stream = stream;
		this.pos = 0;
	}
	
	@Override
	public long getPos() throws IOException {
		return this.pos;
	}

	@Override
	public void seek(long newPos) throws IOException {
		while (this.pos < newPos) {
			this.read();
		}
	}

	@Override
	public boolean seekToNewSource(long arg0) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public int read() throws IOException {
		++this.pos;
		return this.stream.read();
	}

	@Override
	public int read(long position, byte[] buffer, int offset, int length)
			throws IOException {
		if (position < this.pos) {
			this.seek(position);
		}
		
		int i;
		for (i = 0; i < length; ++i) {
			int byteRead = this.read();
			if (byteRead == -1)
				break;
			buffer[offset + i] = (byte) byteRead;
		}
		return i;
	}

	@Override
	public void readFully(long position, byte[] buffer) throws IOException {
		this.readFully(position, buffer, 0, buffer.length);
	}

	@Override
	public void readFully(long position, byte[] buffer, int offset, int length)
			throws IOException {
		int bytesRead = this.read(position, buffer, offset, length);
		if (bytesRead < length) {
			throw new EOFException("Attempted to readFully past the end of the file.");
		}
	}
	
}