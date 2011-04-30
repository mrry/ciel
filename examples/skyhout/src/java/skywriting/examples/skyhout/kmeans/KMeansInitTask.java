package skywriting.examples.skyhout.kmeans;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansInitTask implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {

		int numVectors = Integer.parseInt(Ciel.args[0]);
		int numDimensions = Integer.parseInt(Ciel.args[1]);
		int k = Integer.parseInt(Ciel.args[2]);
		int numPartitions = Integer.parseInt(Ciel.args[3]);
		double epsilon = Double.parseDouble(Ciel.args[4]);
		boolean doCache = Boolean.parseBoolean(Ciel.args[5]);

		//Reference randomData = Ciel.spawn(new KMeansDataGenerator(numVectors, numDimensions), null, 1)[0];
		Reference[] dataPartitions = new Reference[numPartitions];
		for (int i = 0; i < numPartitions; ++i) {
		    dataPartitions[i] = Ciel.spawn(new KMeansDataGenerator(numVectors / numPartitions, numDimensions, i), null, 1)[0];
		}

		Reference initClusters = Ciel.spawn(new KMeansHead(dataPartitions[0], k, numDimensions), null, 1)[0];
		
		Reference[] partialSumsRefs = new Reference[numPartitions];
		for (int i = 0; i < numPartitions; ++i) {
			partialSumsRefs[i] = Ciel.spawn(new KMeansMapTask(dataPartitions[i], initClusters, doCache), null, 1)[0];
		}
		
		Ciel.tailSpawn(new KMeansReduceTask(dataPartitions, partialSumsRefs, initClusters, 0, epsilon, doCache), null);
		
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

}
