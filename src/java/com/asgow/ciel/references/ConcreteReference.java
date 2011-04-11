/*
 * Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */
package com.asgow.ciel.references;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class ConcreteReference extends Reference {

	private final HashSet<Netloc> locationHints;
	private final long sizeHint;
	
	public ConcreteReference (String id, long sizeHint) {
		super(id);
		this.locationHints = new HashSet<Netloc>();
		this.sizeHint = sizeHint; 
	}
	
	public ConcreteReference (String id, long sizeHint, Iterable<Netloc> locations) {
		this(id, sizeHint);
		for (Netloc location : locations) {
			this.addLocation(location);
		}
	}
	
	public ConcreteReference (String id, long sizeHint, Netloc location) {
		this(id, sizeHint);
		this.addLocation(location);
	}
	
	public ConcreteReference (JsonArray refTuple) {
		this(refTuple.get(1).getAsString(), refTuple.get(2).getAsLong());
		for (JsonElement elem : refTuple.get(3).getAsJsonArray()) {
			this.addLocation(new Netloc(elem.getAsString()));
		}
	}
	
	public void addLocation(Netloc netloc) {
		this.locationHints.add(netloc);
	}
	
	public Iterable<Netloc> getLocationHints() {
		return Collections.unmodifiableSet(this.locationHints);
	}
	
	public long getSizeHint() {
		return this.sizeHint;
	}
	
	public boolean isConsumable() {
		return true;
	}
	
	public static final JsonPrimitive IDENTIFIER = new JsonPrimitive("c2");
	public JsonObject toJson() {
		JsonArray ret = new JsonArray();
		ret.add(IDENTIFIER);
		ret.add(new JsonPrimitive(this.getId()));
		ret.add(new JsonPrimitive(this.getSizeHint()));
		JsonArray hints = new JsonArray();
		for (Netloc hint : this.locationHints) {
			hints.add(new JsonPrimitive(hint.toString()));
		}
		ret.add(hints);
		return Reference.wrapAsReference(ret);
	}
	
	public String toString() {
		return "ConcreteReference(" + this.getId() + ")";
	}
	
}
