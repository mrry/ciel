package skywriting.examples.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansDataPartitioner implements FirstClassJavaTask {

	private Reference dataRef;
	private int numPartitions;
	private int numDimensions;
	
	public KMeansDataPartitioner(Reference dataRef, int numPartitions, int numDimensions) {
		this.dataRef = dataRef;
		this.numPartitions = numPartitions;
		this.numDimensions = numDimensions;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataRef };
	}

	@Override
	public void invoke() throws Exception {

		WritableReference[] outRefs = new WritableReference[this.numPartitions];
		DataOutputStream[] outStreams = new DataOutputStream[this.numPartitions];
		for (int i = 0; i < this.numPartitions; ++i) {
			outRefs[i] = Ciel.RPC.getOutputFilename(i);
			outStreams[i] = new DataOutputStream(outRefs[i].open());
		}

		DataInputStream inStream = new DataInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataRef)));
		
		int count = 0;
		try {
			while (true) {
			
				for (int i = 0; i < this.numDimensions; ++i) {
					outStreams[count % this.numPartitions].writeDouble(inStream.readDouble());
				}
				
			}
		} catch (EOFException eofe) {
			inStream.close();
		}
		
		for (int i = 0; i < this.numPartitions; ++i) {
			outStreams[i].close();
			//Ciel.RPC.closeOutput(i);
		}
		
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

}
