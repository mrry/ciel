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

import java.io.*;

import uk.co.mrry.mercator.task.Task;

public class GrepReducer2 implements Task {

	IntWritable[] valHeads;
	Text[] wordHeads;
	DataInputStream[] inputStreams;
	
	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {
		int nOutputs = outputs.length;
		int nInputs = inputs.length;
		DataInputStream[] dis = new DataInputStream[nInputs];
		DataOutputStream[] dos = new DataOutputStream[nOutputs];
		inputStreams = dis;
		
		for(int i = 0; i < nInputs; i++) {
			dis[i] = new DataInputStream(new BufferedInputStream(inputs[i]));
		}
		
		for(int i = 0; i < nOutputs; i++) {
			dos[i] = new DataOutputStream(new BufferedOutputStream(outputs[i]));
		}
		
		valHeads = new IntWritable[inputs.length];
		wordHeads = new Text[inputs.length];
		for (int i = 0; i < inputs.length; i++) {
			valHeads[i] = new IntWritable();
			wordHeads[i] = new Text();
		}
		
		try {
			// Initialise arrays
			for (int i = 0; i < inputs.length; i++) {
				try {
					valHeads[i].readFields(inputStreams[i]);
					wordHeads[i].readFields(inputStreams[i]);
				} catch (EOFException e) {
					valHeads[i] = null;
					wordHeads[i] = null;
					continue;
				}
			}

			// Main iteration
			Pair<IntWritable, Text> pair;
			while ((pair = getNextPair(dis)) != null) {
				pair.getLeft().write(dos[0]);
				pair.getRight().write(dos[0]);
				System.out.println(pair.getLeft() + ": " + pair.getRight());
			}

			for (DataOutputStream d : dos)
				d.close();
	
			} catch (IOException e) {
				System.out.println("IOException while running reducer");
				e.printStackTrace();
				System.exit(1);
			}

	}
	
	
	private Pair<IntWritable, Text> getNextPair(DataInputStream[] inputs) {
		Pair<IntWritable, Text> pair;
		
		int maxID = -1;
		int maxVal = -1;
		
		if (inputs.length < 1)
			return null;
		
		for (int i = 0; i < inputs.length; i++) {
			if (valHeads[i] == null) {
				continue;
			} else {
				int a = valHeads[i].get();
				
				if (a > maxVal) {
					maxID = i;
					maxVal = a;
				} 
			}
		}
		//System.out.println("choose " + maxID + " : " + maxVal);
		
		if (maxID < 0) return null;
		
		if (valHeads[maxID] != null && wordHeads[maxID] != null) {
			pair = new Pair<IntWritable, Text>(new IntWritable(valHeads[maxID].get()), new Text(wordHeads[maxID].toString()));

			try {
				valHeads[maxID].readFields(inputStreams[maxID]);
				wordHeads[maxID].readFields(inputStreams[maxID]);
			} catch (EOFException e) {
				valHeads[maxID] = null;
				wordHeads[maxID] = null;
			} catch (IOException e) {
				System.out.println("IOException while loading from reducer input");
				e.printStackTrace();
				System.exit(1);
			}
			
			return pair;
		} else {
			return null;
		}
		
	}
	
	
	class Pair<L,R> {
		
		L left;
		R right;
		
		Pair() {
			left = null;
			right = null;
		}
		
		Pair(L l, R r) {
			left = l;
			right = r;
		}
		
		L getLeft() {
			return left;
		}
		
		R getRight() {
			return right;
		}
		
		void setLeft(L l) {
			left = l;
		}
		
		void setRight(R r) {
			right = r;
		}
		
	}
	

	public static void main(String[] args) throws Exception {

		int nReducers2 = 1;
		int nReducers1 = 4;
	    InputStream[] fis = new InputStream[nReducers1];
	    FileOutputStream[] fos = new FileOutputStream[1];
	
	    for (int i = 0; i < nReducers2; i++) {
	    
		    for (int j = 0; j < nReducers1; j++) {
		    	fis[j] = new FileInputStream("grep_reduce1_out_" + j);
		    }
	    	fos[0] = new FileOutputStream("grep_reduce2_out_" + i);
	
		    GrepReducer2 m = new GrepReducer2();
		    m.invoke(fis, fos, args);
	    }
	}
}



