package skywriting.examples.skyhout.kmeans;

import java.io.EOFException;
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

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import skywriting.examples.skyhout.common.SortedPartitionedOutputCollector;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class KMeansReduceTask implements Task {

	private DistanceMeasure measure;
	
	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos,
			String[] args) {

		try {
				
			assert args.length == 1;
			double convergenceDelta = Double.parseDouble(args[0]);
	
			Configuration conf = new Configuration();
			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
			new WritableSerialization();

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
			OutputStreamWriter convergedOutput = new OutputStreamWriter(fos[1]);
			convergedOutput.write(Boolean.toString(kmrc.areAllConverged()));
			convergedOutput.close();
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe);
		}
		
	}

}
