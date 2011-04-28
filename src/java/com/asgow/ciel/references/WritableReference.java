package com.asgow.ciel.references;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.io.CielOutputStream;

public class WritableReference {

	private final String filename;
	private final int outputIndex;
	private final boolean may_omit_size;
	private Reference complete_ref;
	
	public WritableReference(String filename, int outputIndex, boolean may_omit_size) {
		this.filename = filename;
		this.outputIndex = outputIndex;
		this.may_omit_size = may_omit_size;
	}
	
	public String getFilename() {
		return this.filename;
	}
	
	public void close(int final_size) {
		this.complete_ref = Ciel.RPC.closeOutput(this.outputIndex, final_size);
	}
	
	public void close() throws Exception {
		if(!this.may_omit_size) {
			throw new Exception("Must specify a size when closing WritableReference, as it was opened with may_pipe=true");
		}
		else {
			this.complete_ref = Ciel.RPC.closeOutput(this.outputIndex);
		}
	}
	
	public Reference getCompletedRef() {
		return this.complete_ref;
	}
	
	public int getIndex() {
		return this.outputIndex;
	}
	
	public OutputStream open() throws IOException {
		return new CielOutputStream(this);
	}
	
}
