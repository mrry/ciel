package com.asgow.ciel.tasks;

import com.asgow.ciel.references.Reference;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class StdinoutTaskInformation implements TaskInformation {

	private final Reference[] inputs;
	private final String[] commandLine;
	
	public StdinoutTaskInformation(Reference[] inputs, String[] commandLine) {
		this.inputs = inputs;
		this.commandLine = commandLine;
	}

	@Override
	public JsonObject toJson() {
		JsonObject ret = new JsonObject();
		ret.add("executor_name", new JsonPrimitive("stdinout"));
		ret.add("small_task", new JsonPrimitive(false));
		ret.add("n_outputs", new JsonPrimitive(1));
		
		JsonObject args = new JsonObject();
		
		JsonArray inputs = new JsonArray();
		if (this.inputs != null) {
			for (Reference input : this.inputs) {
				inputs.add(input.toJson());
			}
		}
		args.add("inputs", inputs);
		JsonArray commandLine = new JsonArray();
		for (String arg : this.commandLine) {
			commandLine.add(new JsonPrimitive(arg));
		}
		args.add("command_line", commandLine);
		ret.add("args", args);
		return ret;
	}
	
}
