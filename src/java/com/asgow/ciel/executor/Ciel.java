package com.asgow.ciel.executor;

import com.asgow.ciel.references.Reference;
import com.asgow.ciel.rpc.WorkerRpc;

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
	
	
}
