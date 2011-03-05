package com.asgow.ciel.references;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class WritableReference {

	private final String filename;
	
	public WritableReference(String filename) {
		this.filename = filename;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
	public OutputStream open() throws IOException {
		return new FileOutputStream(this.filename);
	}
	
}
