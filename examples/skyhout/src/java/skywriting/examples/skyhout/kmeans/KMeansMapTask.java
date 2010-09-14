package skywriting.examples.skyhout.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansInfo;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import skywriting.examples.skyhout.common.OutputCollector;
import skywriting.examples.skyhout.common.PartialHashOutputCollector;
import uk.co.mrry.mercator.task.Task;

public class KMeansMapTask implements Task {

	private DistanceMeasure measure;
	
	/**
	 * Iterates over all clusters and identifies the one closes to the given point
	. Distance measure used is
	 * configured at creation time of .
	 * 
	 * @param point
	 *          a point to find a cluster for.
	 * @param clusters
	 *          a List<Cluster> to test.
	 */
	public void emitPointToNearestCluster(Vector point,
			List<Cluster> clusters,
			OutputCollector<Text,KMeansInfo> output) throws IOException {
		Cluster nearestCluster = null;
		double nearestDistance = Double.MAX_VALUE;
		for (Cluster cluster : clusters) {
			Vector clusterCenter = cluster.getCenter();
			double distance = this.measure.distance(clusterCenter.getLengthSquared(), clusterCenter, point);
			if ((distance < nearestDistance) || (nearestCluster == null)) {
				nearestCluster = cluster;
				nearestDistance = distance;
			}
		}
		// emit only clusterID
		output.collect(new Text(nearestCluster.getIdentifier()), new KMeansInfo(1, point));
	}
	    
	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos,
			String[] args) {
		
		this.measure = new EuclideanDistanceMeasure();
		
		assert fis.length == 2;
		DataInputStream mapInput = new DataInputStream(fis[0]);
		DataInputStream clustersInput = new DataInputStream(fis[1]);
		
		DataOutputStream[] mapOutputs = new DataOutputStream[fos.length];
		for (int i = 0; i < fos.length; ++i) {
			mapOutputs[i] = new DataOutputStream(fos[i]);
		}
		
		try {
		
			List<Cluster> clusters = new ArrayList<Cluster>();
			while (true) {
				try {
					Cluster curr = new Cluster();
					curr.readFields(clustersInput);
					clusters.add(curr);
				} catch (EOFException eofe) {
					break;
				}
			}
			clustersInput.close();
			
			PartialHashOutputCollector<Text, KMeansInfo> output = new PartialHashOutputCollector<Text, KMeansInfo>(mapOutputs, Integer.MAX_VALUE, new skywriting.examples.skyhout.kmeans.KMeansCombiner());
			
			Text currentID = new Text();
			VectorWritable currentVector = new VectorWritable(); 
			while (true) {
				try {
					currentID.readFields(mapInput);
					currentVector.readFields(mapInput);
				} catch (EOFException eofe) {
					break;
				}
				this.emitPointToNearestCluster(currentVector.get(), clusters, output);
			}
			output.flushAll();
			
			for (DataOutputStream mapOutput : mapOutputs) {
				mapOutput.close();
			}
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe);
		}
		
	}


}
