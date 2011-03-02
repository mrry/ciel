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

import com.asgow.ciel.protocol.CielProtos.Reference.Builder;
import com.asgow.ciel.protocol.CielProtos.Reference.ReferenceType;

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
	
	public StreamReference(com.asgow.ciel.protocol.CielProtos.Reference ref) {
		this(ref.getId(), new Netloc(ref.getLocationHints(0)));
	}

	@Override
	public boolean isConsumable() {
		return true;
	}
	
	public Netloc getNetloc() {
		return this.location;
	}

	@Override
	public Builder buildProtoBuf(Builder builder) {
		return builder.setType(ReferenceType.STREAM).addLocationHints(this.location.asProtoBuf());
	}
	
}
