package com.asgow.ciel.executor;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import com.asgow.ciel.references.ConcreteReference;
import com.asgow.ciel.references.Netloc;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.ValueReference;

public class SimpleExecutionContext implements ExecutionContext {

	private URLClassLoader classloader;
	
	public SimpleExecutionContext() {
		this.classloader = null;
	}
	
	@Override
	public <T> T getObjectForReference(Reference ref, Class<T> targetClass) {
		assert ref instanceof ValueReference;
		return ((ValueReference) ref).getValueAsObject(targetClass);
	}
	
	@Override
	public URL getURLForReference(Reference ref) {
		assert ref instanceof ConcreteReference;
		Netloc arbitraryNetloc = ((ConcreteReference) ref).getLocationHints().iterator().next();
		try {
			return new URL("http://" + arbitraryNetloc.getHostname() + ":" + arbitraryNetloc.getPort() + "/data/" + ref.getId());
		} catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setJarURLs(URL[] urls) {
		this.classloader = new URLClassLoader(urls);
	}

	@Override
	public ClassLoader getClassLoader() {
		if (this.classloader == null) {
			throw new IllegalStateException("Tried to getClassLoader() before setting the JAR URLs");
		}
		return this.classloader;
	}

}
