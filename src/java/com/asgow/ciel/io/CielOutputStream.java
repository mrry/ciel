package com.asgow.ciel.io;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.FileOutputStream;

import com.asgow.ciel.references.WritableReference;

public class CielOutputStream extends FileOutputStream {

	private WritableReference ref;
    private long bytesWritten;
    private boolean closed;    
	
	public CielOutputStream(WritableReference ref) throws FileNotFoundException {
		super(ref.getFilename());
		this.ref = ref;
		this.bytesWritten = 0;
		this.closed = false;
	}
	
	@Override
	public void write(int arg0) throws IOException {
		super.write(arg0);
		bytesWritten++;
	}

	@Override
	public void close() throws IOException {
		if(!closed) {
			super.close();
			ref.close(this.bytesWritten);
			this.closed = true;
		}
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		super.write(b, off, len);
		bytesWritten += len;
	}

	@Override
	public void write(byte[] b) throws IOException {
		this.write(b, 0, b.length);
	}

}
