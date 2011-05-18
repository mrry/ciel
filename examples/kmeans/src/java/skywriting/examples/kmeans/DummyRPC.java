package skywriting.examples.kmeans;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.asgow.ciel.references.DummyWritableReference;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.rpc.ShutdownException;
import com.asgow.ciel.rpc.WorkerRpc;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.tasks.TaskInformation;
import com.google.gson.JsonElement;

public class DummyRPC implements WorkerRpc {

	private OutputStream[] outputs;
	
	public DummyRPC(OutputStream[] outputs) {
		this.outputs = outputs;
	}
	
	@Override
	public void closeAsyncInput(String id, int chunkSize) {
		// TODO Auto-generated method stub

	}

	@Override
	public Reference closeNewObject(WritableReference wref) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reference closeOutput(int index) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reference closeOutput(int index, long finalSize) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void error(String errorMessage) {
		System.err.println(errorMessage);
		System.exit(-1);
	}

	@Override
	public void exit(boolean fixed) {
		System.exit(0);
	}

	@Override
	public String getFilenameForReference(Reference ref) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFilenameForReference(Reference ref, boolean makeSweetheart) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void getFixedContinuationTask() {
		// TODO Auto-generated method stub

	}

	@Override
	public WritableReference getNewObjectFilename(String refPrefix) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WritableReference getOutputFilename(int index) {
		return new DummyWritableReference(this.outputs[index], index);
	}

	@Override
	public WritableReference getOutputFilename(int index, boolean mayStream,
			boolean mayPipe, boolean makeLocalSweetheart) {
		return new DummyWritableReference(this.outputs[index], index);
	}

	@Override
	public InputStream getStreamForReference(Reference ref, int chunkSize,
			boolean soleConsumer, boolean makeSweetheart, boolean mustBlock)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getStreamForReference(Reference ref, int chunkSize)
			throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InputStream getStreamForReference(Reference ref) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FirstClassJavaTask getTask() throws ShutdownException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void log(String logMessage) {
		// TODO Auto-generated method stub
		System.err.println(logMessage);
	}

	@Override
	public Reference packageLookup(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Reference[] spawnTask(TaskInformation taskInfo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void tailSpawnRaw(JsonElement e) {
		// TODO Auto-generated method stub

	}

	@Override
	public void tailSpawnTask(TaskInformation taskInfo) {
		// TODO Auto-generated method stub

	}

	@Override
	public Reference tryPackageLookup(String key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public WaitAsyncInputResponse waitAsyncInput(String refid, boolean eof,
			long bytes) {
		// TODO Auto-generated method stub
		return null;
	}

}
