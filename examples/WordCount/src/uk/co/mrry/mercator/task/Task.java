package uk.co.mrry.mercator.task;

import java.io.InputStream;
import java.io.OutputStream;

public interface Task {

	void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args);
	
}
