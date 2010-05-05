import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import uk.co.mrry.mercator.task.Task;

public class SWTeraBucketer implements Task {
	
	private static class TextPair {
		public Text key;
		public Text value;
	}
	
	private static class StreamPusher extends Thread {
		
		private Object[] pairs;
		private OutputStream out;
		private DataOutputStream dataOut;
		public Exception e;
		
		public StreamPusher(Object[] pairs, OutputStream out) {
			this.pairs = pairs;
			this.out = out;
			this.dataOut = new DataOutputStream(new BufferedOutputStream(out));
		}
		
		public void run() {

			try {
				for(Object o : pairs) {
					TextPair t = (TextPair)o;
					t.key.write(this.dataOut);
					t.value.write(this.dataOut);
				}
				this.dataOut.close();
				this.out.close();
			}
			catch(IOException e) {
				System.err.println("Error writing: " + e);
				this.e = e;
			}
			
		}
		
	}
	
	private static class ISTextArray implements IndexedSortable {

		private Object[] myArray;
		
		public ISTextArray(Object[] p) {
			myArray = p;
		}
		
		@Override
		public int compare(int i, int j) {
		
			return ((TextPair)myArray[i]).key.compareTo(((TextPair)myArray[j]).key);
			
		}

		@Override
		public void swap(int i, int j) {

			Object temp = myArray[i];
			myArray[i] = myArray[j];
			myArray[j] = temp;
			
		}
		
	}

	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {
		
		/* Expected: 
		 * Two inputs:
		 * 0: The partition descriptors written by SWTeraSampler
		 * 1: A sequence of lines to be parsed and bucketed
		 * N outputs:
		 * (One per partition)
		 * One argument:
		 * 0: The number of reducers
		 */
		
		int nPartitions = Integer.parseInt(args[0]);
		DataInputStream dis = null;
		if(nPartitions > 1) {
		    dis = new DataInputStream(inputs[0]);
		}
		Text[] boundaries = new Text[nPartitions - 1];
		
		for(int i = 0; i < (nPartitions - 1); i++) {
			
			try {
				Text splitValue = new Text();		
				splitValue.readFields(dis);
				System.out.printf("Split %d is at %s\n", i, splitValue.toString());
				boundaries[i] = splitValue; 
			}
			catch(IOException e) {
				System.err.println("Exception reading partition " + i + ": " + e);
				System.exit(2);
			}
			
		}
		
		TotalOrderPartitioner part = null;
		if(nPartitions > 1) {
		    part = new TotalOrderPartitioner();
		    part.configure(boundaries);
		}
		
		ArrayList<TextPair>[] outBuffers = new ArrayList[nPartitions];
		for(int i = 0; i < nPartitions; i++) {
			outBuffers[i] = new ArrayList<TextPair>();
		}
		
		RecordReader<Text, Text> reader = null;
		try {
			reader = new TeraInputFormat.TeraRecordReader(inputs[1]);
		}
		catch(IOException e) {
			System.err.println("Exception opening input: " + e);
			System.exit(2);
		}
		Text key = new Text();
		Text value = new Text();		
		try {
			while(reader.next(key, value)) {
			    int thisPart;
			    if(nPartitions > 1) {
				thisPart = part.getPartition(key, value, nPartitions);
			    }
			    else {
				thisPart = 0;
			    }
			    TextPair entry = new TextPair();
			    entry.key = key;
			    entry.value = value;
			    outBuffers[thisPart].add(entry);
			    key = new Text();
			    value = new Text();
			}
		}
		catch(IOException e) {
			System.err.println("Exception reading records: " + e);
			System.exit(2);
		}
		
		StreamPusher[] pushThreads = new StreamPusher[nPartitions];
		
		for(int i = 0; i < nPartitions; i++) {
			
			Object[] pairs = outBuffers[i].toArray();
			if(outBuffers[i].size() > 0)
				new QuickSort().sort(new ISTextArray(pairs), 0, pairs.length);
			pushThreads[i] = new StreamPusher(pairs, outputs[i]);
			
		}
		
		for(int i = 0; i < nPartitions; i++) {
			
			pushThreads[i].start();
			
		}
		
		for(int i = 0; i < nPartitions; i++) {
			
			try {
				pushThreads[i].join();
				if(pushThreads[i].e != null) {
					System.err.println("A pusher thread failed with exception " + pushThreads[i].e);
					System.exit(2);
				}
			}
			catch(Exception e) {
				System.err.println("Joining a pusher thread failed with exception " + pushThreads[i].e);
				System.exit(2);				
			}
			
		}
		
	}

}
