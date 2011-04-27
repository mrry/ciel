package com.asgow.ciel.references;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class ValueReference extends Reference {

	private final byte[] value;
	
	public ValueReference(String id, byte[] value) {
		super(id);
		this.value = value;
	}
	
	public ValueReference(JsonArray refTuple) {
		this(refTuple.get(1).getAsString(), refTuple.get(2).getAsString().getBytes());
	}
	
	public <T> T getValueAsObject(Class<T> clazz) {
		Object ret = null;
		try {
			ret = new ObjectInputStream(new ByteArrayInputStream(this.value)).readObject();
			return clazz.cast(ret);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	public boolean isConsumable() {
		return true;
	}

	public static final JsonPrimitive IDENTIFIER = new JsonPrimitive("val");
	public JsonObject toJson() {
		JsonArray ret = new JsonArray();
		ret.add(IDENTIFIER);
		ret.add(new JsonPrimitive(this.getId()));
		ret.add(new JsonPrimitive(new String(this.value)));
		return Reference.wrapAsReference(ret);
	}
	
}
