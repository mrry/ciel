package skywriting.examples.skyhout.kmeans;

import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansInfo;

import skywriting.examples.skyhout.common.Combiner;

public class KMeansCombiner implements Combiner<KMeansInfo> {
	
	@Override
	public KMeansInfo combine(KMeansInfo oldValue, KMeansInfo newValue) {
		Cluster cluster = new Cluster();
		cluster.addPoints(oldValue.getPoints(), oldValue.getPointTotal());
		cluster.addPoints(newValue.getPoints(), newValue.getPointTotal());
		return new KMeansInfo(cluster.getNumPoints(), cluster.getPointTotal());
	}

	@Override
	public KMeansInfo combineInit(KMeansInfo initVal) {
		return new KMeansInfo(initVal.getPoints(), initVal.getPointTotal());
	}

}
