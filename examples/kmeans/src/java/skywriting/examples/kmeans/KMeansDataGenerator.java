package skywriting.examples.kmeans;

import java.io.DataOutputStream;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Random;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansDataGenerator implements FirstClassJavaTask {

	private int numVectors;
	private int numDimensions;
	private int seed;
	
	public KMeansDataGenerator(int numVectors, int numDimensions, int seed) {
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.seed = seed;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		double minValue = -1000000.0;
		double maxValue = 1000000.0;
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(out.open(), 1048576));
		
		Random rand = new Random(this.seed);
		
		for (int i = 0; i < this.numVectors; ++i) {
			for (int j = 0; j < this.numDimensions; ++j) {
				dos.writeDouble((rand.nextDouble() * (maxValue - minValue)) + minValue);
			}
		}

		dos.close();
		//Ciel.RPC.closeOutput(0);
	}

	@Override
	public void setup() {
		;
	}

	public static void main(String[] args) {
		try {
			int numVectors = Integer.parseInt(args[0]);
			int numDimensions = Integer.parseInt(args[1]);
			int seed = Integer.parseInt(args[2]);
			Ciel.RPC = new DummyRPC(new OutputStream[] { new FileOutputStream(args[3]) });
			new KMeansDataGenerator(numVectors, numDimensions, seed).invoke();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
}
