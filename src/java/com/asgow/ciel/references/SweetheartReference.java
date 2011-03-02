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

import com.asgow.ciel.protocol.CielProtos.Reference;

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
	
	public SweetheartReference(Reference ref) {
		super(ref);
		this.sweetheartLocation = new Netloc(ref.getSweetheart());
	}
	
	public Netloc getSweetheartLocation() {
		return this.sweetheartLocation;
	}
	
	public com.asgow.ciel.protocol.CielProtos.Reference.Builder buildProtoBuf(com.asgow.ciel.protocol.CielProtos.Reference.Builder builder) {
		return super.buildProtoBuf(builder).setSweetheart(this.sweetheartLocation.asProtoBuf());
	}
	
}
