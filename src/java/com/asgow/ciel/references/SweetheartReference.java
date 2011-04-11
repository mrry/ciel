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
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public final class SweetheartReference extends ConcreteReference {

	private final Netloc sweetheartLocation;
	
	public SweetheartReference(String id, long sizeHint, Netloc sweetheartLocation) {
		super(id, sizeHint);
		this.sweetheartLocation = sweetheartLocation;
		this.addLocation(sweetheartLocation);
	}
	
	public SweetheartReference(String id, long sizeHint, Netloc sweetheartLocation, Iterable<Netloc> locations) {
		super(id, sizeHint, locations);
		this.sweetheartLocation = sweetheartLocation;
		this.addLocation(sweetheartLocation);
	}
	
	public SweetheartReference(JsonArray refTuple) {
		this(refTuple.get(1).getAsString(), refTuple.get(3).getAsLong(), new Netloc(refTuple.get(2).getAsString()));
		for (JsonElement elem : refTuple.get(4).getAsJsonArray()) {
			this.addLocation(new Netloc(elem.getAsString()));
		}
	}
	
	public Netloc getSweetheartLocation() {
		return this.sweetheartLocation;
	}
	
	public static final JsonPrimitive IDENTIFIER = new JsonPrimitive("<3");
	public JsonObject toJson() {
		JsonArray ret = new JsonArray();
		ret.add(IDENTIFIER);
		ret.add(new JsonPrimitive(this.getId()));
		ret.add(new JsonPrimitive(this.sweetheartLocation.toString()));
		ret.add(new JsonPrimitive(this.getSizeHint()));
		JsonArray hints = new JsonArray();
		for (Netloc hint : this.getLocationHints()) {
			hints.add(new JsonPrimitive(hint.toString()));
		}
		ret.add(hints);
		return com.asgow.ciel.references.Reference.wrapAsReference(ret);
	}
	
}
