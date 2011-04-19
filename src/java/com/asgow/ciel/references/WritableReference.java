package com.asgow.ciel.references;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;

public class WritableReference {

	private class ReferenceOutputStream extends FileOutputStream {

		public ReferenceOutputStream() throws FileNotFoundException {
			super(WritableReference.this.filename);
		}
		
	}
	
	private final String filename;
	private final int outputIndex;
	
	public WritableReference(String filename, int outputIndex) {
		this.filename = filename;
		this.outputIndex = outputIndex;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
	public int getIndex() {
		return this.outputIndex;
	}
	
	public OutputStream open() throws IOException {
		return new ReferenceOutputStream();
	}
	
}
