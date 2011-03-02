package com.asgow.ciel.executor;

import java.io.OutputStream;

public interface CielTaskOutput {

	OutputStream getAsStream();
	OutputStream getAsStream(boolean allowStreamingConsumers);
	
	void write(Object o);
	void write(Object o, boolean publish);
	
	void write(boolean b);
	void write(boolean b, boolean publish);

	void write(byte b);
	void write(byte b, boolean publish);
	
	void write(short s);
	void write(short s, boolean publish);
	
	void write(long l);
	void write(long l, boolean publish);
	
	void write(int i);
	void write(int i, boolean publish);

	void write(String s);
	void write(String s, boolean publish);
	
	void write(float f);
	void write(float f, boolean publish);
	
	void write(double d);
	void write(double d, boolean publish);
	
}
