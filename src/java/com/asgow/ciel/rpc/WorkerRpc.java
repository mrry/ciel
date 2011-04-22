package com.asgow.ciel.rpc;

import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.tasks.TaskInformation;

public interface WorkerRpc {

	FirstClassJavaTask getTask() throws ShutdownException;
	
	Reference[] spawnTask(TaskInformation taskInfo);
	
	void tailSpawnTask(TaskInformation taskInfo);
	
	Reference[] blockOn(Reference... refs);
	
	String getFilenameForReference(Reference ref);
	
	WritableReference getOutputFilename(int index);
	
	WritableReference getNewObjectFilename(String refPrefix);
	
	Reference closeOutput(int index);
	
	Reference closeNewObject(WritableReference wref);

	void error(String errorMessage);
	
	void exit();
	
}
