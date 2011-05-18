package com.asgow.ciel.rpc;

import java.io.IOException;
import java.io.InputStream;

import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.tasks.TaskInformation;
import com.google.gson.JsonElement;

public interface WorkerRpc {

	FirstClassJavaTask getTask() throws ShutdownException;
	
	void getFixedContinuationTask();
	
	Reference[] spawnTask(TaskInformation taskInfo);
	
	void tailSpawnTask(TaskInformation taskInfo);
	
	void tailSpawnRaw(JsonElement e);
	
	String getFilenameForReference(Reference ref);
	
	WritableReference getOutputFilename(int index);
	
	WritableReference getNewObjectFilename(String refPrefix);
	
	Reference closeOutput(int index);

	Reference closeOutput(int index, long final_size);

	Reference closeNewObject(WritableReference wref);

	Reference packageLookup(String key);
	
	Reference tryPackageLookup(String key);
	
	void log(String logMessage);
	
	void error(String errorMessage);
	
	void exit(boolean fixed);

	public String getFilenameForReference(Reference ref, boolean makeSweetheart);
	
	InputStream getStreamForReference(Reference ref, int chunk_size,
			boolean sole_consumer, boolean make_sweetheart, boolean must_block) throws IOException;

	void closeAsyncInput(String id, int chunk_size);
	
	static class WaitAsyncInputResponse {
		
		public int size;
		public boolean done;
		public boolean success;
		
		public WaitAsyncInputResponse(int size, boolean done, boolean success) {
			this.size = size;
			this.done = done;
			this.success = success;
		}
		
	}
	
	WaitAsyncInputResponse waitAsyncInput(String refid, boolean eof, long bytes);

	InputStream getStreamForReference(Reference ref, int chunk_size)
			throws IOException;

	InputStream getStreamForReference(Reference ref) throws IOException;

	WritableReference getOutputFilename(int index, boolean may_stream,
			boolean may_pipe, boolean make_local_sweetheart);

}
