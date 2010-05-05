
import uk.co.mrry.mercator.task.Task;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SWTeraSampler implements Task {

	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {

		int nBucketers = Integer.parseInt(args[0]);
		int nPartitions = Integer.parseInt(args[1]);
		int nRecordsExpected = Integer.parseInt(args[2]);
		int recordsBetweenBoundaries = nRecordsExpected / nPartitions;

		DataInputStream[] dis = new DataInputStream[nBucketers];
		
		for(int i = 0; i < nBucketers; i++) {
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
		    int i = 1;
		    int boundariesEmitted = 0;
		    while((boundariesEmitted < (nPartitions - 1)) && iter.next()) {
			Text key;
			if((i % recordsBetweenBoundaries) == 0) {
			    key = iter.getKey();
			    key.write(dos);
			    boundariesEmitted++;
			}
			i++;
		    }
		    dos.close();
		    if(boundariesEmitted != (nPartitions - 1)) {
			System.err.printf("Emitted %d boundaries (expected %d), probably due to short input (got %d records, expected %d)\n", 
					  boundariesEmitted, nPartitions - 1, i, nRecordsExpected);
			System.exit(2);
		    }

		}
		catch(IOException e) {
			System.err.println("Exception during iteration / writing: " + e);
			System.exit(2);
		}
		
	}
	
}
