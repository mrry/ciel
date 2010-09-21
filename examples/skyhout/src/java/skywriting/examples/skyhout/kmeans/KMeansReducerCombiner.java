package skywriting.examples.skyhout.kmeans;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansInfo;
import org.apache.mahout.common.distance.DistanceMeasure;

import skywriting.examples.skyhout.common.Combiner;

public class KMeansReducerCombiner implements Combiner<Text, KMeansInfo, KMeansInfo, Cluster> {
	
	private boolean allConverged;
	private Map<String, Cluster> oldClusterMap;
	private DistanceMeasure measure;
	private double convergenceDelta;
	
	public KMeansReducerCombiner(Map<String, Cluster> oldClusterMap, DistanceMeasure measure, double convergenceDelta) {
		this.oldClusterMap = oldClusterMap;
		this.allConverged = true;
		this.measure = measure;
		this.convergenceDelta = convergenceDelta;
	}
	
	public boolean areAllConverged() {
		return this.allConverged;
	}
	
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
	
	public Cluster combineFinal(Text key, KMeansInfo oldVal) throws IOException {
		System.out.println("Processing cluster " + key);
		Cluster cluster = oldClusterMap.get(key.toString());
		cluster.addPoints(oldVal.getPoints(), oldVal.getPointTotal());
		boolean clusterConverged = cluster.computeConvergence(this.measure, convergenceDelta);
		allConverged &= clusterConverged;
		return cluster;
	}

}
