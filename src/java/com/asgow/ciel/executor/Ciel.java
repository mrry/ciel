package com.asgow.ciel.executor;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.charset.Charset;

import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.rpc.WorkerRpc;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.tasks.FirstClassJavaTaskInformation;
import com.asgow.ciel.tasks.SingleOutputTask;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public final class Ciel {

	/**
	 * A convenience method for accessing the worker RPC bindings. This must
	 * be configured in the Java2Executor setup phase.
	 */
	public static WorkerRpc RPC = null;
	
	/**
	 * This is the classloader used to load the user-supplied classes. Any
	 * reflective calls inside user-code should use this classloader.
	 */
	public static ClassLoader CLASSLOADER = null;
	
	/**
	 * This is the array of references that comprise the classpath for this task. It
	 * is inherited when spawning further tasks.
	 */
	public static Reference[] jarLib = null;
	
	/**
	 * This is the argument vector that may be supplied for externally-invoked tasks.
	 */
	public static String[] args = null;
	
	private static Charset CHARSET = Charset.forName("UTF-8");
	
	public static Reference[] spawn(FirstClassJavaTask taskObject, String[] args, int numOutputs) throws IOException {
		WritableReference objOut = Ciel.RPC.getNewObjectFilename("obj");
		ObjectOutputStream oos = new ObjectOutputStream(objOut.open());
		oos.writeObject(taskObject);
		oos.close();
		Reference objRef = Ciel.RPC.closeOutput(objOut.getIndex());
		
		FirstClassJavaTaskInformation fcjti = new FirstClassJavaTaskInformation(objRef, Ciel.jarLib, args, numOutputs);
		for (Reference dependency : taskObject.getDependencies()) {
			fcjti.addDependency(dependency);
		}
		return Ciel.RPC.spawnTask(fcjti);
	}
	
	public static Reference[] spawn(Class<? extends FirstClassJavaTask> taskClass, String[] args, int numOutputs) {
		FirstClassJavaTaskInformation fcjti = new FirstClassJavaTaskInformation(taskClass, Ciel.jarLib, args, numOutputs);
		return Ciel.RPC.spawnTask(fcjti);
	}
	
	public static Reference spawn(Class<? extends SingleOutputTask<? extends Serializable>> taskClass, String[] args) {
		return Ciel.spawn(taskClass, args, 1)[0];
	}

	public static void tailSpawn(FirstClassJavaTask taskObject, String[] args) throws IOException {
		WritableReference objOut = Ciel.RPC.getNewObjectFilename("obj");
		ObjectOutputStream oos = new ObjectOutputStream(objOut.open());
		oos.writeObject(taskObject);
		oos.close();
		Reference objRef = Ciel.RPC.closeOutput(objOut.getIndex());
		
		FirstClassJavaTaskInformation fcjti = new FirstClassJavaTaskInformation(objRef, Ciel.jarLib, args);
		for (Reference dependency : taskObject.getDependencies()) {
			fcjti.addDependency(dependency);
		}
		Ciel.RPC.tailSpawnTask(fcjti);
	}
	
	public static void returnInt(int value) throws IOException {
		Ciel.returnInt(value, 0);
	}
	
	public static void returnInt(int value, int index) throws IOException {
		WritableReference retOut = Ciel.RPC.getOutputFilename(index);
		ObjectOutputStream oos = new ObjectOutputStream(retOut.open());
		oos.write(value);
		oos.close();
		Ciel.RPC.closeOutput(index);
	}

	
	public static void returnPlainString(String value) throws IOException {
		Ciel.returnPlainString(value, 0);
	}
	
	public static void returnPlainString(String value, int index) throws IOException {
		WritableReference retOut = Ciel.RPC.getOutputFilename(index);
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(retOut.open(), Ciel.CHARSET));
		pw.print(value);
		pw.close();
		Ciel.RPC.closeOutput(index);
	}
	
	public static void blockOn(Reference... refs) {
		JsonArray deps = new JsonArray();
		for (Reference ref : refs) {
			deps.add(ref.toJson());
		}
		JsonObject args = new JsonObject();
		args.addProperty("executor", "java2");
		args.add("extra_dependencies", deps);
		args.addProperty("is_fixed", true);
		Ciel.RPC.tailSpawnRaw(args);
		
		args = new JsonObject();
		args.addProperty("keep_process", "must_keep");
		Ciel.RPC.exit(true);
		
		Ciel.RPC.getFixedContinuationTask();
		
	}

	
}
