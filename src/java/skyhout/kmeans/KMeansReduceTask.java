package skyhout.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansInfo;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;

import skyhout.common.SortedPartialHashOutputCollector;
import uk.co.mrry.mercator.task.Task;

public class KMeansReduceTask implements Task {

	private DistanceMeasure measure;
	
	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos,
			String[] args) {

		assert args.length == 1;
		double convergenceDelta = Double.parseDouble(args[0]);

		this.measure = new EuclideanDistanceMeasure();
		
		assert fis.length == 2;
		
		assert fis.length == 2;
		DataInputStream[] reduceInputs = new DataInputStream[fis.length - 1]; 
		for (int i = 0; i < reduceInputs.length; ++i) {
			reduceInputs[i] = new DataInputStream(fis[i]);
		}
		
		DataInputStream oldClustersInput = new DataInputStream(fis[fis.length - 1]);
		
		assert fos.length == 2;
		DataOutputStream reduceOutput = new DataOutputStream(fos[0]);
		OutputStreamWriter convergedOutput = new OutputStreamWriter(fos[1]);
		
		SortedPartialHashOutputCollector<Text, KMeansInfo> inputCollector = new SortedPartialHashOutputCollector<Text, KMeansInfo>(new KMeansCombiner());

		
		try {
		
			HashMap<String, Cluster> oldClusterMap = new HashMap<String, Cluster>();
			while (true) {
				try {
					Cluster curr = new Cluster();
					curr.readFields(oldClustersInput);
					oldClusterMap.put(curr.getIdentifier(), curr);
				} catch (EOFException eofe) {
					break;
				}
			}
			oldClustersInput.close();
			
			Text currentReduceKey = new Text();
			KMeansInfo currentReduceValue = new KMeansInfo();

			for (int i = 0; i < reduceInputs.length; ++i) {
				while (true) {
					try {
						currentReduceKey.readFields(reduceInputs[i]);
						currentReduceValue.readFields(reduceInputs[i]);
					} catch (EOFException eofe) {
						break;
					}
					inputCollector.collect(currentReduceKey, currentReduceValue);
				}
			}
			
			boolean allConverged = true;
			for (Map.Entry<Text, KMeansInfo> inputEntry : inputCollector) {
				Cluster cluster = oldClusterMap.get(inputEntry.getKey().toString());
				KMeansInfo value = inputEntry.getValue();
				cluster.addPoints(value.getPoints(), value.getPointTotal());
				boolean clusterConverged = cluster.computeConvergence(this.measure, convergenceDelta);
				allConverged &= clusterConverged;
				cluster.write(reduceOutput);
			}

			convergedOutput.write(Boolean.toString(allConverged));
			
			reduceOutput.close();
			convergedOutput.close();
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe);
		}
		
	}

}
