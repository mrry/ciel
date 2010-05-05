import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;


public class SWTeraMerger implements Task {
	
	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {
		
		/*
		 * Expected: an input from each mapper, a single output, and one
		 * argument: the number of mappers.
		 * Will read serialized Texts as given by SWTeraBucketer, and write
		 * lines. 
		 */
		
		int nPartitions = Integer.parseInt(args[0]);
		DataInputStream[] dis = new DataInputStream[nPartitions];
		final byte[] newLine = "\r\n".getBytes();
		
		for(int i = 0; i < nPartitions; i++) {
			dis[i] = new DataInputStream(new BufferedInputStream(inputs[i]));
		}
		
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(outputs[0]));
		TextPairIterator iter = null;
		try {
			iter = Merger.merge(dis);
		}
		catch(IOException e) {
			System.err.println("Exception during merge: " + e);
			System.exit(2);
		}
		try {
			while(iter.next()) {
				Text key = iter.getKey();
				Text value = iter.getValue();
				dos.write(key.getBytes(), 0, key.getLength());
			    dos.write(value.getBytes(), 0, value.getLength());
			    dos.write(newLine, 0, newLine.length);
			}
			dos.close();
		}
		catch(IOException e) {
			System.err.println("Exception during iteration / writing: " + e);
			System.exit(2);
		}
		
	}

}
