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

import com.asgow.ciel.protocol.CielProtos.NetworkLocation;

public class Netloc implements Comparable<Netloc> {

	private final String hostname;
	private final short port;
	
	public Netloc(String hostname, short port) {
		this.hostname = hostname;
		this.port = port;
	}
	
	public Netloc(NetworkLocation netloc) {
		this.hostname = netloc.getHostname();
		this.port = (short) netloc.getPort();
	}
	
	public Netloc(String hostnameAndPort) {
		// TODO: Make this more robust by using Scanner and a regular expression.
		String[] splits = hostnameAndPort.split(":");
		if (splits.length == 1) {
			throw new IllegalArgumentException("Network location must contain a port: " + hostnameAndPort);
		} else if (splits.length > 2) {
			throw new IllegalArgumentException("Illegally formatted hostname:port pair: " + hostnameAndPort);
		}
		this.hostname = splits[0];
		this.port = Short.valueOf(splits[1]);
	}
	
	public String getHostname() {
		return this.hostname;
	}
	
	public short getPort() {
		return this.port;
	}
	
	public NetworkLocation asProtoBuf() {
		return NetworkLocation.newBuilder().setHostname(this.hostname).setPort(this.port).build();
	}
	
	public int hashCode() {
		return this.hostname.hashCode() + this.port;
	}

	@Override
	public int compareTo(Netloc o) {
		return this.hostname.compareTo(o.hostname) + (this.port - o.port);
	}
	
	public String toString() {
		return this.hostname + ":" + this.port;
	}
	
}
