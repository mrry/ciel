package com.asgow.ciel.references;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class CompletedReference extends Reference {

	public CompletedReference(String id) {
		super(id);
	}
	
	public CompletedReference(JsonArray refTuple) {
		this(refTuple.get(0).getAsString());
	}
	
	@Override
	public boolean isConsumable() {
		return false;
	}

	public static final JsonPrimitive IDENTIFIER = new JsonPrimitive("completed2");

	@Override
	public JsonObject toJson() {
		JsonArray ret = new JsonArray();
		ret.add(IDENTIFIER);
		ret.add(new JsonPrimitive(this.getId()));
		return Reference.wrapAsReference(ret);
	}

}
