package com.asgow.ciel.tasks;

import java.io.ObjectOutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;

public abstract class SingleOutputTask<T> implements FirstClassJavaTask {

	private static final long serialVersionUID = 2685291728220990030L;

	@Override
	public void invoke() throws Exception {
		T result = this.run();
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		ObjectOutputStream oos = new ObjectOutputStream(out.open());
		oos.writeObject(result);
		oos.close();
	}

	@Override
	public void setup() {
		;
	}

	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	public abstract T run();
	
}
