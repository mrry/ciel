package com.asgow.ciel.io;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.rpc.WorkerRpc.WaitAsyncInputResponse;

public class CielInputStream extends InputStream {
	
	private InputStream real_is;
	private boolean done;
	private boolean succeeded;
	private boolean blocking;
	private boolean must_close;
	private int size;
	private long bytes_read;
	private Reference ref;
	private int chunk_size;
	
	public CielInputStream(Reference ref, int chunk_size, String filename, boolean done, boolean blocking, int initial_size) {
		
		try {
			this.real_is = new FileInputStream(filename);
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		this.ref = ref;
		this.done = done;
		if(done)
			this.succeeded = true;
		this.chunk_size = chunk_size;
		this.must_close = !done;
		this.blocking = blocking;
		this.size = initial_size;
		this.bytes_read = 0;
		
	}

	private void waitForMore() throws IOException {
	        long target_size = this.bytes_read + this.chunk_size;
		WaitAsyncInputResponse response = Ciel.RPC.waitAsyncInput(this.ref.getId(), false, target_size);
		this.size = response.size;
		this.done = response.done;
		this.succeeded = response.success;
		if(!this.succeeded) {
			throw new IOException("Failure reported waiting for ref " + this.ref.getId());
		}
	}
	
	@Override
	public int read() throws IOException {
		for(int i = 0; i < 10; i++) {
			int ret = this.real_is.read();
			if(ret == -1) {
				if(this.done || this.blocking) {
					return ret; 
				}
				else {
					waitForMore();
				}
			}
			else {
				bytes_read++;
				return ret;
			}
		}
		throw new IOException("Too many retries reading " + this.ref.toString());
	}

	@Override
	public int available() throws IOException {
		return this.real_is.available();
	}

	@Override
	public void close() throws IOException {
		this.real_is.close();
		if(this.must_close) {
			Ciel.RPC.closeAsyncInput(this.ref.getId(), this.chunk_size);
		}
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		for(int i = 0; i < 10; i++) {
			int ret = this.real_is.read(b, off, len);
			if(ret == -1) {
				if(this.done || this.blocking) {
					return ret;
				}
				else {
					waitForMore();
				}
			}
			else {
				bytes_read += ret;
				return ret;
			}
		}
		throw new IOException("Too many retries trying to read " + this.ref.getId());
	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	@Override
	public long skip(long n) throws IOException {
		for(int i = 0; i < 10; i++) {
			long ret = this.real_is.skip(n);
			if(ret == 0) {
				if(this.done || this.blocking) {
					return 0;
				}
				else {
					waitForMore();
				}
			}
			else {
				bytes_read += ret;
				return ret;
			}
		}
		throw new IOException("Too many retries skipping " + n + " bytes of ref " + this.ref.getId());
	}
	
	

}
