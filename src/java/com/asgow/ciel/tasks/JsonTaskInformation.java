package com.asgow.ciel.tasks;

import com.google.gson.JsonObject;

public class JsonTaskInformation implements TaskInformation {

	private final JsonObject info;
	
	public JsonTaskInformation(JsonObject info) {
		this.info = info;
	}
	
	@Override
	public JsonObject toJson() {
		return this.info;
	}

}
