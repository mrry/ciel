package com.asgow.ciel.tasks;

import java.util.LinkedList;

import com.asgow.ciel.references.Reference;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class FirstClassJavaTaskInformation implements TaskInformation {

	private final String className;
	private final Reference objectRef;
	private final LinkedList<Reference> dependencies;
	private final Reference[] jarLib;
	private final String[] args;
	private final int numOutputs;
	
	public FirstClassJavaTaskInformation(Class<? extends FirstClassJavaTask> clazz, Reference[] jarLib, String[] args, int numOutputs) {
		this.className = clazz.getName();
		this.objectRef = null;
		this.jarLib = jarLib;
		this.args = args;
		this.numOutputs = numOutputs;
		this.dependencies = new LinkedList<Reference>();
	}
	
	public FirstClassJavaTaskInformation(Reference objectRef, Reference[] jarLib, String[] args, int numOutputs) {
		this.className = null;
		this.objectRef = objectRef;
		this.jarLib = jarLib;
		this.args = args;
		this.numOutputs = numOutputs;
		this.dependencies = new LinkedList<Reference>();
	}
	
	public FirstClassJavaTaskInformation(Reference objectRef, Reference[] jarLib, String[] args) {
		this(objectRef, jarLib, args, -1);
	}
	
	public void addDependency(Reference ref) {
		this.dependencies.add(ref);
	}
	
	@Override
	public JsonObject toJson() {
		JsonObject ret = new JsonObject();
		
		ret.add("executor_name", new JsonPrimitive("java2"));
		
		if (this.numOutputs > 0) {
			ret.add("n_outputs", new JsonPrimitive(this.numOutputs));
		}
		
		if (this.className != null) {
			ret.add("class_name", new JsonPrimitive(this.className));
		} else if (this.objectRef != null) {
			ret.add("object_ref", this.objectRef.toJson());
		} else {
			assert false;
		}
		
		JsonArray jsonJarLib = new JsonArray();
		for (Reference jarRef : this.jarLib) {
			jsonJarLib.add(jarRef.toJson());
		}
		ret.add("jar_lib", jsonJarLib);
		
		JsonArray jsonDependencies = new JsonArray();
		for (Reference depRef : this.dependencies) {
			jsonDependencies.add(depRef.toJson());
		}
		ret.add("extra_dependencies", jsonDependencies);
		
		if (this.args != null) {
			JsonArray jsonArgs = new JsonArray();
			for (String arg : args) {
				jsonArgs.add(new JsonPrimitive(arg));
			}
			ret.add("args", jsonArgs);
		}
		
		return ret;
		
	}

}
