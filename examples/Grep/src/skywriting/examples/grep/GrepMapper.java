package skywriting.examples.grep;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;	
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.*;
import java.net.URL;

import uk.co.mrry.mercator.task.Task;

public class GrepMapper implements Task {

	private final static IntWritable one = new IntWritable(1);

	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {

		int nReducers = outputs.length;
		int nInputs = inputs.length;
		BufferedReader[] dis = new BufferedReader[nInputs];
		DataOutputStream[] dos = new DataOutputStream[nReducers];
		
		// Set up the regex
		Pattern regEx = Pattern.compile(args[0]);
		
		for(int i = 0; i < nInputs; i++) {
			dis[i] = new BufferedReader(new InputStreamReader(inputs[i]));
		}
		
		try {
			for(int i = 0; i < nReducers; i++) {
				dos[i] = new DataOutputStream(new BufferedOutputStream(outputs[i]));
				dos[i].write(0);
			}

			String line;
			IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(dos, nReducers, 1000, comb);
			while ((line = dis[0].readLine()) != null) { 
			    //System.err.println(line);
				Matcher match = regEx.matcher(line);
				
				while (match.find()) {
					Text word = new Text(match.group());
					//System.err.println("found " + match.group());
					outMap.collect(word, one);
				}
			}
			outMap.flushAll();
			for (DataOutputStream d : dos) 
				d.close();
		} catch (IOException e) {
			System.err.println("IOException while running mapper");
			e.printStackTrace();
			System.exit(1);
		}

	}
	

	public static void main(String[] args) throws Exception {

		int nMappers = 4;
		int nReducers = 4;
		
	    InputStream[] fis = new InputStream[1];
	    FileOutputStream[] fos = new FileOutputStream[nReducers];
	
	    for (int i = 0; i < nMappers; i++) {
	    	URL u = new URL("http://www.cl.cam.ac.uk/~ms705/sw/wc_input_" + i);
	    	fis[0] = u.openStream();

		    for (int j = 0; j < fos.length; j++) {
		    	fos[j] = new FileOutputStream("grep_map_out_" + i + "_" + j);
		    }
		
		    
		    GrepMapper m = new GrepMapper();
		    m.invoke(fis, fos, args);
	    }
	    
	}
}
