package com.asgow.ciel.executor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.HashSet;

import com.asgow.ciel.references.CielFuture;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.rpc.WorkerRpc;
import com.asgow.ciel.tasks.FirstClassJavaTask;
import com.asgow.ciel.tasks.FirstClassJavaTaskInformation;
import com.asgow.ciel.tasks.SingleOutputTask;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

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
	
	public static HashSet<URL> seenJars = new HashSet<URL>();
	
	/**
	 * This is the array of references that comprise the classpath for this task. It
	 * is inherited when spawning further tasks.
	 */
	public static Reference[] jarLib = null;
	
	/**
	 * This is the argument vector that may be supplied for externally-invoked tasks.
	 */
	public static String[] args = null;
	
	/**
	 * The soft cache, for users to potentially persist useful objects derived of references.
	 */
	public static SoftCache softCache = null;
	
	private static Charset CHARSET = Charset.forName("UTF-8");
	
	public static void log(String logMessage) {
		Ciel.RPC.log(logMessage);
	}
	
	public static Reference[] spawn(FirstClassJavaTask taskObject, String[] args, int numOutputs) throws IOException {
		WritableReference objOut = Ciel.RPC.getNewObjectFilename("obj");
		System.out.println("Spawn: create new output " + objOut.getIndex());
		ObjectOutputStream oos = new ObjectOutputStream(objOut.open());
		oos.writeObject(taskObject);
		System.out.println("Spawn: closing new output " + objOut.getIndex());
		oos.close();
		Reference objRef = objOut.getCompletedRef();
		
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
	
	public static <T> CielFuture<T> spawn(Class<? extends SingleOutputTask<T>> taskClass, String[] args) {
		return new CielFuture<T>(Ciel.spawn(taskClass, args, 1)[0]);
	}
	
	public static <T> CielFuture<T> spawnSingle(SingleOutputTask<T> taskObject) throws IOException {
		return new CielFuture<T>(Ciel.spawn(taskObject, null, 1)[0]);
	}

	public static void tailSpawn(FirstClassJavaTask taskObject, String[] args) throws IOException {
		WritableReference objOut = Ciel.RPC.getNewObjectFilename("obj");
		System.out.println("TailSpawn: opened new output " + objOut.getIndex());
		ObjectOutputStream oos = new ObjectOutputStream(objOut.open());
		oos.writeObject(taskObject);
		System.out.println("TailSpawn: closing new output " + objOut.getIndex());
		oos.close();
		Reference objRef = objOut.getCompletedRef();
		
		FirstClassJavaTaskInformation fcjti = new FirstClassJavaTaskInformation(objRef, Ciel.jarLib, args);
		for (Reference dependency : taskObject.getDependencies()) {
			fcjti.addDependency(dependency);
		}
		Ciel.RPC.tailSpawnTask(fcjti);
	}
	
	public static <T> void returnObject(T result) throws IOException {
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		ObjectOutputStream oos = new ObjectOutputStream(out.open());
		oos.writeObject(result);
		oos.close();
	}
	
	public static void returnInt(int value) throws IOException {
		Ciel.returnInt(value, 0);
	}
	
	public static void returnInt(int value, int index) throws IOException {
		WritableReference retOut = Ciel.RPC.getOutputFilename(index);
		ObjectOutputStream oos = new ObjectOutputStream(retOut.open());
		oos.write(value);
		oos.close();
	}

	
	public static void returnPlainString(String value) throws IOException {
		Ciel.returnPlainString(value, 0);
	}
	
	public static void returnPlainString(String value, int index) throws IOException {
		WritableReference retOut = Ciel.RPC.getOutputFilename(index);
		PrintWriter pw = new PrintWriter(new OutputStreamWriter(retOut.open(), Ciel.CHARSET));
		pw.print(value);
		pw.close();
	}
	
	public static void blockOn(Reference... refs) {
		JsonArray deps = new JsonArray();
		for (Reference ref : refs) {
			deps.add(ref.toJson());
		}
		JsonObject args = new JsonObject();
		args.addProperty("executor_name", "java2");
		args.add("extra_dependencies", deps);
		args.addProperty("is_fixed", true);
		Ciel.RPC.tailSpawnRaw(args);
		
		Ciel.RPC.exit(true);
		
		Ciel.RPC.getFixedContinuationTask();
		
	}

	public static String stringOfRef(Reference ref) {
		
		String filename = Ciel.RPC.getFilenameForReference(ref);
		FileInputStream stream = null;
		try {
			stream = new FileInputStream(filename);
		}
		catch(FileNotFoundException e) {
			// Can't happen
		}
		
		try {
			byte[] buffer = new byte[1024];
			StringBuilder builder = new StringBuilder();
			int this_read;
			while((this_read = stream.read(buffer)) != -1) {
				String new_str = new String(buffer, 0, this_read);
				builder.append(new_str);
			}
			
			stream.close();
			return builder.toString();
		}
		catch(Exception e) {
			throw new RuntimeException(e);
		}
		
	}
	
	public static Reference[] getRefsFromPackage(String key) {
		Reference indexRef = Ciel.RPC.tryPackageLookup(key);
		if (indexRef == null) {
			return null;
		}
		JsonArray indexJsonArray = new JsonParser().parse(Ciel.stringOfRef(indexRef)).getAsJsonArray();
		Reference[] ret = new Reference[indexJsonArray.size()];
		for (int i = 0; i < ret.length; ++i) {
			ret[i] = Reference.fromJson(indexJsonArray.get(i).getAsJsonObject());
		}
		return ret;
	}
	
}
