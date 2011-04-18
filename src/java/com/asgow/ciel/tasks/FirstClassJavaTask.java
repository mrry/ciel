package com.asgow.ciel.tasks;

import java.io.Serializable;

public interface FirstClassJavaTask extends Serializable {

	void setup();
	void invoke() throws Exception;
	
}
