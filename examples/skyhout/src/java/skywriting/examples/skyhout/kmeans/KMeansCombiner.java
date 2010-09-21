package skywriting.examples.skyhout.kmeans;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansInfo;

import skywriting.examples.skyhout.common.Combiner;

public class KMeansCombiner implements Combiner<Text, KMeansInfo, KMeansInfo, KMeansInfo> {
	
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
	
	public KMeansInfo combineFinal(Text key, KMeansInfo oldVal) {
		return oldVal;
	}

}
