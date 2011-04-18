package com.asgow.ciel.tasks;

import java.io.Serializable;

import com.asgow.ciel.references.Reference;

public interface FirstClassJavaTask extends Serializable {

	void setup();
	void invoke() throws Exception;
	
	Reference[] getDependencies();
	
}
