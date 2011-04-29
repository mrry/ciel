package skywriting.examples.skyhout.kmeans;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansConfigKeys;
import org.apache.mahout.clustering.kmeans.KMeansInfo;
import org.apache.mahout.clustering.kmeans.KMeansMapper;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

import skywriting.examples.skyhout.common.PartialHashOutputCollector;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class KMeansMapTask implements FirstClassJavaTask {

	private final Reference dataPartitionRef;
	private final Reference clustersRef;
	
	private static final DistanceMeasure measure = new SquaredEuclideanDistanceMeasure();;
	
	public KMeansMapTask(Reference dataPartitionRef, Reference clustersRef) {
		this.dataPartitionRef = dataPartitionRef;
		this.clustersRef = clustersRef;
	}

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
		//System.err.println("Emitting point to cluster " + nearestCluster.getIdentifier());
		output.collect(new Text(nearestCluster.getIdentifier()), new KMeansInfo(1, point));
		//System.err.println("Emitted point to cluster " + nearestCluster.getIdentifier());
	}
	    
	@Override
	public void invoke() throws Exception {
		
		InputStream[] fis = new InputStream[] { new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef)),
												new FileInputStream(Ciel.RPC.getFilenameForReference(this.clustersRef)) };
		
		WritableReference partialSumsOut = Ciel.RPC.getOutputFilename(0);
		OutputStream[] fos = new OutputStream[] { partialSumsOut.open() };

		Configuration conf = new Configuration();
		conf.setClassLoader(Ciel.CLASSLOADER);
		conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
		new WritableSerialization();

		SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);
		
		List<Cluster> clusters = new ArrayList<Cluster>();
		Text currentClusterID = new Text();
		
		for (int i = 1; i < fis.length; ++i) {
			SequenceFile.Reader clusterReader = new SequenceFile.Reader(fs, new Path("/in/" + i), conf);
			Cluster currentCluster = new Cluster();
			while (true) {
				try {
					boolean isMore = clusterReader.next(currentClusterID, currentCluster);
					if (!isMore) break;
				} catch (EOFException eofe) {
					break;
				}
				clusters.add(currentCluster);
				currentCluster = new Cluster();
			}
			clusterReader.close();
		}
			
		PartialHashOutputCollector<Text, KMeansInfo, KMeansInfo, KMeansInfo> output = new PartialHashOutputCollector<Text, KMeansInfo, KMeansInfo, KMeansInfo>(fs, conf, Text.class, KMeansInfo.class, Integer.MAX_VALUE, new skywriting.examples.skyhout.kmeans.KMeansCombiner());
		
		Text currentID = new Text();
		
		SequenceFile.Reader mapReader = new SequenceFile.Reader(fs, new Path("/in/0"), conf);
		
		VectorWritable currentVector = mapReader.getValueClass().asSubclass(VectorWritable.class).newInstance(); 

		int i = 0;
		while (true) {
			try {
				//System.err.println("About to read");
				boolean isMore = mapReader.next(currentID, currentVector);
				//System.err.println("Read");
				if (!isMore) break;
			} catch (EOFException eofe) {
				break;
			}
			//System.err.println(currentID);
			//System.err.println("Done a point: " + i);
			i++;
			//if (i % 1000 == 0) System.out.println(i);
			this.emitPointToNearestCluster(currentVector.get(), clusters, output);
			//System.out.println(i);
		}
		mapReader.close();
		output.flushAll();
		output.closeWriters();

	}

	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef, this.clustersRef };
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}


}
