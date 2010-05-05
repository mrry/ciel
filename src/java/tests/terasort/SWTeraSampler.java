
import uk.co.mrry.mercator.task.Task;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SWTeraSampler implements Task {

	public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {

		int inputLength = Integer.parseInt(args[0]);
		int partitions = Integer.parseInt(args[1]);
		try {
			TeraInputFormat.writePartitionFile(outputs[0], inputs[0], inputLength, partitions);
		}
		catch(IOException e) {
			System.err.println("Exception: " + e);
			// Damn. Looks like the interface should really mention an exception so it's possible to signal failure!
		}
		
	}
	
}
