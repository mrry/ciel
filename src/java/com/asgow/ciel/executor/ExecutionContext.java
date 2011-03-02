package com.asgow.ciel.executor;

import java.net.URL;

import com.asgow.ciel.references.Reference;

public interface ExecutionContext {

	<T> T getObjectForReference(Reference ref, Class<T> targetClass);
	
	URL getURLForReference(Reference ref);
	
	void setJarURLs(URL[] jarURLs);
	
	ClassLoader getClassLoader();
	
}
