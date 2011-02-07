/*
 * Copyright (c) 2010 Chris Smowton <chris.smowton@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

using System;
using System.Reflection;
using System.Text;
using System.IO;
using System.Collections.Generic;

namespace ciel {

  public class SWDotNetLoader {

    public static void Main(String[] args) {

      if(args[0].Equals("--version")) {
	Console.Out.WriteLine("Ciel Mono bindings v0.1");
	return;
      }

      /* Step 1: Open assemblies and instantiate an appropriate class */

      if(args.Length < 2) {

	Console.Error.WriteLine("Usage: mono SWDotNetLoader.exe assembly [other assemblies...] fully.qualified.classname");
	Environment.Exit(1);

      }

      Assembly contains_main = null;

      try {

	for(int i = 1; i < args.Length; i++) {
	  Assembly loaded = Assembly.LoadFile(args[i]);
	  if(loaded.GetType(args[0]) != null)
	    contains_main = loaded;
	}

      }
      catch(Exception e) {

	Console.Error.WriteLine("Failed to load an assembly: {0}", e);
	Environment.Exit(1);

      }

      if(contains_main == null) {
	Console.Error.WriteLine("None of the specified assemblies contained a class named {0}", args[0]);
	Environment.Exit(1);
      }
    
      Object target = null;
    
      try {
	target = contains_main.CreateInstance(args[0]);
      }
      catch(Exception e) {

	Console.Error.WriteLine("Failed to create an instance of {0}: {1}", args[0], e);
	Environment.Exit(1);

      }

      Task target_task = null;
  
      try {
	target_task = (Task)target;
      }
      catch(Exception e) {
	Console.Error.WriteLine("Could not cast {0} to Task: {1}", args[0], e);
	Environment.Exit(1);
      }

      /* Step 2: Retrieve I/O files and arguments */

      Console.InputEncoding = Encoding.UTF8;

      StringBuilder sb = new StringBuilder();
      List<String> inputStrings = new List<String>();
      int nextChar;

      try {
      
	while((nextChar = Console.Read()) != -1) {
	
	  if(nextChar == 0) {
	  
	    inputStrings.Add(sb.ToString());
	    sb.Length = 0;
	  
	  }
	  else {
	  
	    sb.Append((char)nextChar);
		    
	  }
	
	}
      
      }
      catch(IOException e) {
      
	Console.Error.WriteLine("IOException reading from stdin: {0}", e);
	Environment.Exit(1);
      
      }

      if(inputStrings.Count == 0) {

	Console.Error.WriteLine("Error: stdin closed before we had received a single null char");
	Environment.Exit(1);

      }

      String[] toks = inputStrings[0].Split(',');

      if(toks.Length != 3) {

	Console.Error.WriteLine("First string argument must have format number,number,number (got {0})", inputStrings[0]);
	Environment.Exit(1);

      }

      int nInputFiles = 0, nOutputFiles = 0, nArgs = 0;

      try {

	nInputFiles = int.Parse(toks[0]);
	nOutputFiles = int.Parse(toks[1]);
	nArgs = int.Parse(toks[2]);

      }
      catch(FormatException e) {
	    
	Console.Error.WriteLine("Failed to parse three integers from {0}: {1}", inputStrings[0], e);
	Environment.Exit(1);

      }

      if((nInputFiles + nOutputFiles + nArgs + 1) != inputStrings.Count) {

	Console.Error.WriteLine("Expected {0} input files, {1} output files, {2} arguments, plus the 1 string-of-counts, but got a total of {3} strings:", nInputFiles, nOutputFiles, nArgs, (nInputFiles + nOutputFiles + nArgs + 1));
	foreach(String s in inputStrings) {
	  Console.Error.WriteLine(s);
	}
	Environment.Exit(1);

      }

      FileStream[] targetInputs = new FileStream[nInputFiles];
      FileStream[] targetOutputs = new FileStream[nOutputFiles];
      String[] targetArgs = inputStrings.GetRange(nInputFiles + nOutputFiles + 1, nArgs).ToArray();

      int j = 0;
      foreach(String s in inputStrings.GetRange(1, nInputFiles)) {
	try {
	  targetInputs[j++] = new FileStream(s, FileMode.Open, FileAccess.Read);
	}
	catch(Exception e) {
	  Console.Error.WriteLine("Failed to open a file for input: {0}", e);
	  Environment.Exit(1);
	}
      }

      j = 0;
      foreach(String s in inputStrings.GetRange(nInputFiles + 1, nOutputFiles)) {
	try {
	  targetOutputs[j++] = new FileStream(s, FileMode.Open, FileAccess.Write);
	}
	catch(Exception e) {
	  Console.Error.WriteLine("Failed to open a file for output: {0}", e);
	  Environment.Exit(1);
	}
      }

      try {
	target_task.invoke(targetInputs, targetOutputs, targetArgs);
      }
      catch(Exception e) {
	Console.Error.WriteLine("Invocation of {0} died with an exception: {1}", args[0], e);      
	Environment.Exit(2);
      }

    }

  }

}
