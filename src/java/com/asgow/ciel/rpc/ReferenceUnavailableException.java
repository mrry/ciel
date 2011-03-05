package com.asgow.ciel.rpc;

import com.asgow.ciel.references.Reference;

public class ReferenceUnavailableException extends RuntimeException {

	private final Reference ref;
	
	public ReferenceUnavailableException(Reference ref) {
		this.ref = ref;
	}
	
	public String toString() {
		return "ReferenceUnavailableException(" + ref + ")";
	}
	
}
