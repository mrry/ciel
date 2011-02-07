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

import java.io.*;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import uk.co.mrry.mercator.task.Task;

public class GrepReducer1 implements Task {

	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {
		int nOutputs = outputs.length;
		int nInputs = inputs.length;
		DataInputStream[] dis = new DataInputStream[nInputs];
		DataOutputStream[] dos = new DataOutputStream[nOutputs];
		
		try {
			for(int i = 0; i < nInputs; i++) {
				dis[i] = new DataInputStream(new BufferedInputStream(inputs[i]));
				int ret = dis[i].read();
				if (ret != 0) throw new IOException("Reading DIS " + i + " got " + ret);
			}
			
			for(int i = 0; i < nOutputs; i++) {
				dos[i] = new DataOutputStream(new BufferedOutputStream(outputs[i]));
				dos[i].write(0);
			}

			IncrementerCombiner comb = new IncrementerCombiner();
			PartialHashOutputCollector<Text, IntWritable> outMap = new PartialHashOutputCollector<Text, IntWritable>(null, 1, Integer.MAX_VALUE, comb);
			
			for (int i = 0; i < dis.length; i++) {
				while (true) {
					Text word = new Text();
					IntWritable value = new IntWritable();
					try {
						word.readFields(dis[i]);
						value.readFields(dis[i]);
					} catch (EOFException e) {
						break;
					}

					System.err.println(word + " = " + value);
					outMap.collect(word, value);
				}
			}
			
			SetInsertCombiner<Text> icomb = new SetInsertCombiner<Text>();
			SortedPartialHashOutputCollector<IntWritable, Set<Text>> sortedMap = new SortedPartialHashOutputCollector<IntWritable, Set<Text>>(icomb);
			
			for (Map.Entry<Text, IntWritable> entry : outMap){
				System.err.println("SORTED " + entry.getValue() + " = " + entry.getKey());
				sortedMap.collect(entry.getValue(), Collections.singleton(entry.getKey()));
			}
			
			for (Map.Entry<IntWritable, Set<Text>> entry : sortedMap.descendingMap().entrySet()) {
				for (Text t : entry.getValue()) {
					entry.getKey().write(dos[0]);
					t.write(dos[0]);
				}
			}
			
			for (DataOutputStream d : dos)
				d.close();
			
		} catch (IOException e) {
			System.err.println("IOException while running reducer" + e);
			e.printStackTrace();
			System.exit(1);
		}

	}
	

	public static void main(String[] args) throws Exception {

		int nReducers = 4;
		int nMappers = 4;
	    InputStream[] fis = new InputStream[nMappers];
	    FileOutputStream[] fos = new FileOutputStream[1];
	
	    for (int i = 0; i < nReducers; i++) {
	    
		    for (int j = 0; j < nMappers; j++) {
		    	fis[j] = new FileInputStream("grep_map_out_" + j + "_" + i);
		    }
		    
	    	fos[0] = new FileOutputStream("grep_reduce1_out_" + i);
	
		    GrepReducer1 m = new GrepReducer1();
		    m.invoke(fis, fos, args);
	    }
	}
}



