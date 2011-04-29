package skywriting.examples.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansHead implements FirstClassJavaTask {

	private final Reference dataPartitionRef;
	private final int k;
	private final int numDimensions;
	
	public KMeansHead(Reference dataPartitionRef, int k, int numDimensions) {
		this.dataPartitionRef = dataPartitionRef;
		this.k = k;
		this.numDimensions = numDimensions;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef };
	}

	@Override
	public void invoke() throws Exception {
		DataInputStream dis = new DataInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef)));
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		DataOutputStream dos = new DataOutputStream(out.open());
		for (int i = 0; i < this.k * this.numDimensions; ++i) {
			dos.writeDouble(dis.readDouble());
		}
		dis.close();
		dos.close();
		//Ciel.RPC.closeOutput(0);
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

}
