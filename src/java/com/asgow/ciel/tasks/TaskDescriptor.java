package com.asgow.ciel.tasks;

import java.util.Collections;
import java.util.HashSet;
import java.util.Vector;

import com.asgow.ciel.protocol.CielProtos.Task;
import com.asgow.ciel.references.Reference;

public final class TaskDescriptor {

	private final String id;
	private final HashSet<Reference> dependencies;
	private final Vector<String> expectedOutputs;
	private final Reference privateData;
	
	public TaskDescriptor(String id, Reference privateData) {
		this.id = id;
		this.dependencies = new HashSet<Reference>();
		this.expectedOutputs = new Vector<String>();
		this.privateData = privateData;
	}
	
	public TaskDescriptor(Task task) {
		this(task.getId(), Reference.fromProtoBuf(task.getTaskPrivate()));
		for (com.asgow.ciel.protocol.CielProtos.Reference ref : task.getDependenciesList()) {
			this.addDependency(Reference.fromProtoBuf(ref));
		}
		this.expectedOutputs.addAll(task.getExpectedOutputsList());
	}
	
	public void addDependency(Reference ref) {
		this.dependencies.add(ref);
	}
	
	public void addExpectedOutput(String id) {
		this.expectedOutputs.add(id);
	}
	
	public Reference getPrivateData() {
		return this.privateData;
	}
	
	public String getId() {
		return this.id;
	}
	
	public Iterable<Reference> getDependencies() {
		return Collections.unmodifiableSet(this.dependencies);
	}
	
	public Iterable<String> getExpectedOutputs() {
		return Collections.unmodifiableList(this.expectedOutputs);
	}
	
	public int numDependencies() {
		return this.dependencies.size();
	}
	
	public int numExpectedOutputs() {
		return this.expectedOutputs.size();
	}
	
	public Task asProtoBuf() {
		Task.Builder builder = Task.newBuilder().setId(this.id);
		for (Reference ref : this.dependencies) {
			builder.addDependencies(ref.asProtoBuf());
		}
		builder.addAllExpectedOutputs(this.expectedOutputs);
		return builder.build();
	}
	
}
