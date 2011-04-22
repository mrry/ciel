package com.asgow.ciel.rpc;

public class ShutdownException extends Exception {

	public String reason;
	
	public ShutdownException(String reason) {
		
		this.reason = reason;
		
	}
	
}
