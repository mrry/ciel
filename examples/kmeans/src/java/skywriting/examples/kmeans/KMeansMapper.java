package skywriting.examples.kmeans;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

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
	private final boolean doCache;
	
	public KMeansMapper(Reference dataPartitionRef, Reference clustersRef, int k, int numDimensions, boolean doCache) {
		this.dataPartitionRef = dataPartitionRef;
		this.clustersRef = clustersRef;
		this.k = k;
		this.numDimensions = numDimensions;
		this.doCache = doCache;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef, this.clustersRef };
	}

	@Override
	public void invoke() throws Exception {
		
		DataInputStream clustersIn = new DataInputStream(new BufferedInputStream(Ciel.RPC.getStreamForReference(this.dataPartitionRef, 1048576, false, true, false), 1048576));
	
		double[][] clusters = new double[this.k][this.numDimensions];
		
		for (int i = 0; i < this.k; ++i) {
			for (int j = 0; j < this.numDimensions; ++j) {
				clusters[i][j] = clustersIn.readDouble();
			}
		}
		
		clustersIn.close();
		
		KMeansMapperResult result = new KMeansMapperResult(this.k, this.numDimensions);
		
		LinkedList<double[]> vectors;
		boolean doRead;
		DataInputStream dataIn;
		Iterator<double[]> vectorIterator;
		
		if (this.doCache) {
			vectors = (LinkedList<double[]>) Ciel.softCache.tryGetCache("fastkmeansin", this.dataPartitionRef);
			if (vectors == null) {
				System.err.println("!!!Cache miss!!!");
				doRead = true;
				vectors = new LinkedList<double[]>();
				vectorIterator = null;
				dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef, true)), 1048576));
			} else {
				System.err.println("!!!Cache hit!!!");
				doRead = false;
				dataIn = null;
				vectorIterator = vectors.iterator();
			}
		} else {
			doRead = true;
			dataIn = new DataInputStream(new BufferedInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef, true)), 1048576));
			vectorIterator = null;
			vectors = null;
		}
		
		double[] currentVector = new double[this.numDimensions];

		int v = 0;
		
		long start = System.currentTimeMillis();
		try {
		
			while (true) {
				
				if (!doRead) {
					if (vectorIterator.hasNext()) {
						currentVector = vectorIterator.next();
					} else {
						break;
					}
				} else {
					for (int j = 0; j < this.numDimensions; ++j) {
						currentVector[j] = dataIn.readDouble();
					}
					if (doCache) {
						vectors.addLast(currentVector);
					}
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
				
				++v;
				result.add(nearestCluster, currentVector);
				
				if (doRead && doCache) {
					currentVector = new double[this.numDimensions];
				}
				
			}
			
		} catch (EOFException eofe) {
			;
		}
		long finish = System.currentTimeMillis();
		System.err.println("*****>>>>> " + (doRead ? "From-disk" : "From-cache") + " loop with " + v + " vectors took " + (finish - start) + " ms");
		
		if (doRead && doCache) {
			Ciel.softCache.putCache(vectors, "fastkmeansin", this.dataPartitionRef);
			System.out.println("!!!Cache put!!!");
		}
		
		Ciel.returnObject(result);
	}
	
	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

	
	
}
