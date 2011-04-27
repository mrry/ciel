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

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * @author dgm36
 *
 */
public final class StreamReference extends Reference {
	
	private final Netloc location;
	
	public StreamReference(String id, Netloc location) {
		super(id);
		this.location = location;
	}

	public StreamReference(JsonArray refTuple) {
		this(refTuple.get(1).getAsString(), new Netloc(refTuple.get(2).getAsJsonArray().get(0).getAsString()));
	}
	
	@Override
	public boolean isConsumable() {
		return true;
	}
	
	public Netloc getNetloc() {
		return this.location;
	}
	
	public static final JsonPrimitive IDENTIFIER = new JsonPrimitive("s2");
	public JsonObject toJson() {
		JsonArray ret = new JsonArray();
		ret.add(IDENTIFIER);
		ret.add(new JsonPrimitive(this.getId()));
		JsonArray hint = new JsonArray();
		hint.add(new JsonPrimitive(this.getNetloc().toString()));
		ret.add(hint);
		return Reference.wrapAsReference(ret);
	}
	
}
