package skywriting.examples.kmeans;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansMapper implements FirstClassJavaTask {

	public static double getSquaredDistance(double[] x, double[] y) {
		double ret = 0.0;
		for (int i = 0; i < x.length; ++i) {
			ret += (y[i] - x[i]) * (y[i] - x[i]);
		}
		return ret;
	}

	
	
	private final Reference dataPartitionRef;
	private final Reference clustersRef;
	private final int k;
	private final int numDimensions;
	
	public KMeansMapper(Reference dataPartitionRef, Reference clustersRef, int k, int numDimensions) {
		this.dataPartitionRef = dataPartitionRef;
		this.clustersRef = clustersRef;
		this.k = k;
		this.numDimensions = numDimensions;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef, this.clustersRef };
	}

	@Override
	public void invoke() throws Exception {
		
		DataInputStream clustersIn = new DataInputStream(new BufferedInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.clustersRef)), 1048576));
	
		double[][] clusters = new double[this.k][this.numDimensions];
		
		for (int i = 0; i < this.k; ++i) {
			for (int j = 0; j < this.numDimensions; ++j) {
				clusters[i][j] = clustersIn.readDouble();
			}
		}
		
		clustersIn.close();
		
		KMeansMapperResult result = new KMeansMapperResult(this.k, this.numDimensions);
		
		DataInputStream dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef)), 1048576));
		
		double[] currentVector = new double[this.numDimensions];

		try {
		
			while (true) {
			
				for (int j = 0; j < this.numDimensions; ++j) {
					currentVector[j] = dataIn.readDouble();
				}
	
				int nearestCluster = -1;
				double minDistance = Double.MAX_VALUE;
				
				for (int i = 0; i < this.k; ++i) {
					double distance = getSquaredDistance(currentVector, clusters[i]);
					if (distance < minDistance) {
						nearestCluster = i;
						minDistance = distance;
					}
				}
				
				result.add(nearestCluster, currentVector);
				
			}
		
		} catch (EOFException eofe) {
			;
		}
		
		Ciel.returnObject(result);
	}
	
	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

	
	
}
