package com.asgow.ciel.executor;

import java.io.InputStream;

public interface CielTaskInput {

	InputStream getAsStream();
	
	Object readObject();
	
	boolean readBoolean();
	
	byte readByte();
	
	short readShort();
	
	long readLong();
	
	int readInt();
	
	String readString();

	float readFloat();
	
	double readDouble();
	
}
