package com.asgow.ciel.references;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.rpc.ReferenceUnavailableException;

public class CielFuture<T> extends FutureReference {

	private static final long serialVersionUID = -4656221550083314411L;

	public CielFuture(Reference ref) {
		super(ref.getId());
	}
	
	@SuppressWarnings("unchecked")
	public T get() {
		try {
			String filename = Ciel.RPC.getFilenameForReference(this);
			FileInputStream fis = new FileInputStream(filename);
			ObjectInputStream ois = new ObjectInputStream(fis);
			return (T) ois.readObject();
		} catch (ReferenceUnavailableException rue) { 
			throw rue;
		} catch (Exception e) {
			System.err.println("Error getting value from future.");
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

}
