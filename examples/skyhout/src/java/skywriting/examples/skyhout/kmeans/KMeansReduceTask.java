package skywriting.examples.skyhout.kmeans;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansInfo;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class KMeansReduceTask implements FirstClassJavaTask {

	private DistanceMeasure measure;
	
	private final Reference[] dataPartitionsRefs;
	private final Reference[] partialSumsRefs;
	private final Reference oldClustersRef;
	private final int iteration;
	private final double convergenceDelta;
	private final boolean doCache;
	
	public KMeansReduceTask(Reference[] dataPartitionRefs, Reference[] partialSumsRefs, Reference oldClustersRef, int iteration, double convergenceDelta, boolean doCache) {
		this.dataPartitionsRefs = dataPartitionRefs;
		this.partialSumsRefs = partialSumsRefs;
		this.oldClustersRef = oldClustersRef;
		this.iteration = iteration;
		this.convergenceDelta = convergenceDelta;
		this.doCache = doCache;
	}
	
	@Override
	public void invoke() throws Exception {
	
		Configuration conf = new Configuration();
		conf.setClassLoader(Ciel.CLASSLOADER);
		conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
		new WritableSerialization();

		FileInputStream[] fis = new FileInputStream[this.partialSumsRefs.length + 1];
		for (int i = 0; i < this.partialSumsRefs.length; ++i) {
			fis[i] = new FileInputStream(Ciel.RPC.getFilenameForReference(this.partialSumsRefs[i]));
		}
		fis[fis.length - 1] = new FileInputStream(Ciel.RPC.getFilenameForReference(this.oldClustersRef));


		WritableReference clustersOut = Ciel.RPC.getNewObjectFilename("clusters");
		OutputStream[] fos = new OutputStream[] { clustersOut.open() };
		
		SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);
		
		this.measure = new SquaredEuclideanDistanceMeasure();
		
		assert fs.numInputs() == 2;
		assert fs.numOutputs() == 2;
		

		HashMap<String, Cluster> oldClusterMap = new HashMap<String, Cluster>();
		SequenceFile.Reader oldClusterReader = new SequenceFile.Reader(fs, new Path("/in/" + (fs.numInputs() - 1)), conf);

		
		while (true) {

			Text id = new Text();
			Cluster curr = new Cluster();
			
			try {
				boolean isMore = oldClusterReader.next(id, curr);
				if (!isMore) break;
			} catch (EOFException eofe) {
				break;
			}
			oldClusterMap.put(curr.getIdentifier(), curr);
			//System.out.println("Putting cluster    " + curr.getIdentifier() + " in oldClusterMap");
		}
			
		oldClusterReader.close();

		
		KMeansReducerCombiner kmrc = new KMeansReducerCombiner(oldClusterMap, measure, convergenceDelta);
		SortedPartitionedOutputCollector<Text, KMeansInfo, KMeansInfo, Cluster> inputCollector = new SortedPartitionedOutputCollector<Text, KMeansInfo, KMeansInfo, Cluster>(fs, new HashPartitioner<Text, KMeansInfo>(), kmrc, Text.class, Cluster.class, 1);
		
		KMeansInfo currentReduceValue = new KMeansInfo();

		for (int i = 0; i < fis.length - 1; ++i) {
			SequenceFile.Reader reduceInputReader = new SequenceFile.Reader(fs, new Path("/in/" + i), conf);
			while (true) {
				Text currentReduceKey = new Text();
				try {
					boolean isMore = reduceInputReader.next(currentReduceKey, currentReduceValue);
					if (!isMore) break;
				} catch (EOFException eofe) {
					break;
				}
				inputCollector.collect(currentReduceKey, currentReduceValue);
			}
		}
		
		inputCollector.close();
		
		if (!kmrc.areAllConverged() && this.iteration <= 10) {
			
			Reference newClustersRef = clustersOut.getCompletedRef();
			
			Reference[] newPartialSumsRefs = new Reference[this.dataPartitionsRefs.length];
			
			for (int i = 0; i < newPartialSumsRefs.length; ++i) {
				System.out.println("dpr[i]" + this.dataPartitionsRefs[i]);
				System.out.println("ncr   " + newClustersRef);
				newPartialSumsRefs[i] = Ciel.spawn(new KMeansMapTask(this.dataPartitionsRefs[i], newClustersRef, this.doCache), null, 1)[0];
			}

			Ciel.tailSpawn(new KMeansReduceTask(this.dataPartitionsRefs, newPartialSumsRefs, newClustersRef, this.iteration + 1, this.convergenceDelta, this.doCache), null);
			
			
		} else {
		
			Ciel.returnPlainString("Finished!");
					
		}
		
	}

	@Override
	public Reference[] getDependencies() {
		Reference[] ret = new Reference[this.partialSumsRefs.length + 1];
		for (int i = 0; i < this.partialSumsRefs.length; ++i) {
			ret[i] = this.partialSumsRefs[i];
		}
		ret[ret.length - 1] = this.oldClustersRef;
		return ret;
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub
		
	}

}
