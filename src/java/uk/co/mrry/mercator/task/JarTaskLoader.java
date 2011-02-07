package uk.co.mrry.mercator.task;

import java.net.URL;
import java.net.MalformedURLException;
import java.io.IOException;
import java.net.URLClassLoader;
import java.io.Reader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.util.ListIterator;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.UnsupportedEncodingException;

public class JarTaskLoader {

	public static ClassLoader CLASSLOADER;
	
    public static void main(String[] args) {

	if(args[0].equals("--version")) {
	    System.out.println("Ciel Java bindings v0.1\n");
	    return;
	}

	if(args.length < 2) {
	    System.err.println("Usage: java JarTaskLoader fully.qualified.TaskClass file:///path/to/jar [further jars...]");
	    System.exit(1);
	}

	/* Step one: try to instantiate the class we've been asked for */

	URL[] urls = new URL[args.length - 1];

	try {
	    for(int i = 1; i < args.length; i++) {
		urls[i-1] = new URL(args[i]);
	    }
	}
	catch(MalformedURLException e) {
	    System.err.println("Malformed URL: " + e.toString());
	}

	URLClassLoader urlcl = new URLClassLoader(urls);
	JarTaskLoader.CLASSLOADER = urlcl;
	Class targetClass = null;
	Object targetInstance = null;
	Task targetTask = null;
	
	
	try {
	    targetClass = urlcl.loadClass(args[0]);
	}
	catch(ClassNotFoundException e) {
	    System.err.println("Class " + args[0] + " not found in given JARs");
	    System.exit(1);
	}
	catch(Exception e) {
	    System.err.println("Error loading class '" + args[0] + "': " + e.toString());
	    System.exit(1);
	}
	try {
	    targetInstance = targetClass.newInstance();
	}
	catch(Exception e) {
	    System.err.println("Error instantiating " + args[0] + ": " + e.toString());
	    System.exit(1);
	}
	try {
	    targetTask = (Task)targetInstance;
	}
	catch(ClassCastException e) {
	    System.err.println("Error: class " + args[0] + " does not implement the Task interface");
	    System.exit(1);
	}
	
	/* Step two: pull a list of input files, output files, and arguments from stdin */

	Reader r = null;
	try {
	    r = new BufferedReader(new InputStreamReader(System.in, "UTF-8"));
	}
	catch(UnsupportedEncodingException e) {
	    System.err.println("Error: JVM does not support UTF-8 encoding");
	    System.exit(1);
	}
	StringBuilder sb = new StringBuilder();
	List<String> inputStrings = new ArrayList<String>();
	int nextChar;

	try {

	    while((nextChar = r.read()) != -1) {

		if(nextChar == 0) {

		    inputStrings.add(sb.toString());
		    sb.setLength(0);
		
		}
		else {
		
		    sb.append((char)nextChar);

		}

	    }

	}
	catch(IOException e) {

	    System.err.println("IOException reading from stdin: " + e.toString());
	    System.exit(1);

	}

	if(inputStrings.size() == 0) {

	    System.err.println("Error: stdin closed before we had received a single null char");
	    System.exit(1);

	}

	StringTokenizer toks = new StringTokenizer(inputStrings.get(0), ",");

	if(toks.countTokens() != 3) {

	    System.err.println("First string argument must have format number,number,number (got " + inputStrings.get(0) + ")");
		System.out.write('Q');
	    System.exit(1);

	}

	int nInputFiles = 0, nOutputFiles = 0, nArgs = 0;

	try {

	    nInputFiles = Integer.parseInt(toks.nextToken());
	    nOutputFiles = Integer.parseInt(toks.nextToken());
	    nArgs = Integer.parseInt(toks.nextToken());

	}
	catch(NoSuchElementException e) {
	    
	    System.err.println("Failed to tokenize the string " + inputStrings.get(0));
		System.out.write('Q');
	    System.exit(1);

	}
	catch(NumberFormatException e) {
	    
	    System.err.println("Failed to parse three integers from " + inputStrings.get(0));
		System.out.write('Q');
	    System.exit(1);

	}

	if((nInputFiles + nOutputFiles + nArgs + 1) != inputStrings.size()) {

	    System.err.printf("Expected %d input files, %d output files, %d arguments, plus the 1 string-of-counts, but got a total of %d strings:\n", nInputFiles, nOutputFiles, nArgs, (nInputFiles + nOutputFiles + nArgs + 1));
	    ListIterator<String> it = inputStrings.listIterator();
	    while(it.hasNext()) {
		System.err.println(it.next());
	    }
		System.out.write('Q');
	    System.exit(1);

	}

	FileInputStream[] targetInputs = new FileInputStream[nInputFiles];
	FileOutputStream[] targetOutputs = new FileOutputStream[nOutputFiles];
	ListIterator<String> it = inputStrings.subList(nInputFiles + nOutputFiles + 1, nInputFiles + nOutputFiles + nArgs + 1).listIterator();
	String[] targetArgs = new String[nArgs];
	int i = 0;
	    
	while(it.hasNext()) {
	    targetArgs[i++] = it.next();
	}

	it = inputStrings.subList(1, nInputFiles + 1).listIterator();
	i = 0;
	while(it.hasNext()) {
	    try {
		targetInputs[i++] = new FileInputStream(it.next());
	    }
	    catch(Exception e) {
		System.err.println("Failed to open a file for input: " + e.toString());
		System.out.write('Q');
		System.exit(1);
	    }
	}

	it = inputStrings.subList(nInputFiles + 1, nInputFiles + nOutputFiles + 1).listIterator();
	i = 0;
	while(it.hasNext()) {
	    try {
		targetOutputs[i++] = new FileOutputStream(it.next());
	    }
	    catch(Exception e) {
		System.err.println("Failed to open a file for output: " + e.toString());		
		System.out.write('Q');
		System.exit(1);
	    }
	}

	System.out.write('S');
	System.out.flush();
	
	try {
	    targetTask.invoke(targetInputs, targetOutputs, targetArgs);
	}
	catch(Exception e) {
	    System.err.println("Invoked task died with an exception: " + e.toString());
	    e.printStackTrace(System.err);
	    System.exit(2);
	}

    }

}