package skywriting.examples.kmeans;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansReducer implements FirstClassJavaTask {

	private final Reference[] partialSumsRefs;
	private final Reference oldClustersRef;
	private final int k;
	private final int numDimensions;
	private final double epsilon;
	private final Reference[] dataPartitionsRefs;
	private final int iteration;
	private final boolean doCache;
	
	public KMeansReducer(Reference[] partialSumsRefs, Reference oldClustersRef, int k, int numDimensions, double epsilon, Reference[] dataPartitionsRefs, int iteration, boolean doCache) {
		this.partialSumsRefs = partialSumsRefs;
		this.oldClustersRef = oldClustersRef;
		this.k = k;
		this.numDimensions = numDimensions;
		this.epsilon = epsilon;
		this.dataPartitionsRefs = dataPartitionsRefs;
		this.iteration = iteration;
		this.doCache = doCache;
	}
	
	@Override
	public Reference[] getDependencies() {
		ArrayList<Reference> retList = new ArrayList<Reference>(this.partialSumsRefs.length + 1);
		for (int i = 0; i < this.partialSumsRefs.length; ++i) {
			retList.add(this.partialSumsRefs[i]);
		}
		retList.add(this.oldClustersRef);
		return retList.toArray(new Reference[this.partialSumsRefs.length + 1]);
	}

	@Override
	public void invoke() throws Exception {
		KMeansMapperResult result = new KMeansMapperResult(this.k, this.numDimensions);
		
		for (Reference pSumRef : this.partialSumsRefs) {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(pSumRef)));
			KMeansMapperResult pSum = (KMeansMapperResult) ois.readObject();
			result.add(pSum);
			ois.close();
		}
		
		result.normalise();
		
		DataInputStream oldClustersIn = new DataInputStream(new BufferedInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.oldClustersRef)), 1048576));
		double[][] oldClusters = new double[this.k][this.numDimensions];
		for (int i = 0; i < this.k; ++i) {
			for (int j = 0; j < this.numDimensions; ++j) {
				oldClusters[i][j] = oldClustersIn.readDouble();
			}
		}
		oldClustersIn.close();
		
		double error = result.error(oldClusters);
		
		System.err.println("Iteration " + this.iteration + "; Error = " + error);
		
		if (error > this.epsilon && this.iteration <= 20) {
		
			WritableReference newClustersOut = Ciel.RPC.getNewObjectFilename("clusters");
			DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(newClustersOut.open(), 1048576));
			
			for (int i = 0; i < this.k; ++i) {
				for (int j = 0; j < result.sums[i].length; ++j) {
					// result has been normalised.
					dos.writeDouble(result.sums[i][j]);
				}
			}
			
			dos.close();
			Reference newClustersRef = newClustersOut.getCompletedRef();
			
			Reference[] newPartialSumsRefs = new Reference[this.dataPartitionsRefs.length];
			
			for (int i = 0; i < newPartialSumsRefs.length; ++i) {
				newPartialSumsRefs[i] = Ciel.spawn(new KMeansMapper(this.dataPartitionsRefs[i], newClustersRef, this.k, this.numDimensions, this.doCache), null, 1)[0];
			}

			Ciel.tailSpawn(new KMeansReducer(newPartialSumsRefs, newClustersRef, this.k, this.numDimensions, this.epsilon, this.dataPartitionsRefs, this.iteration + 1, this.doCache), null);
			
		} else {
		
			Ciel.returnPlainString("Finished!");
			//Ciel.returnObject(result.sums);
		}
		
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

	
}
