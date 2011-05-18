package com.asgow.ciel.references;

import java.io.OutputStream;

public class DummyWritableReference extends WritableReference {

	private final OutputStream out;
	
	public DummyWritableReference(OutputStream out, int index) {
		super(null, index, true);
		this.out = out;
	}
	
	public OutputStream open() {
		return this.out;
	}

	public void close() {
		return;
	}
	
	public void close(long finalSize) {
		return;
	}
	
	public Reference getCompletedRef() {
		throw new UnsupportedOperationException();
	}
	
}
