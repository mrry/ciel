package skyhout.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.VectorWritable;

import uk.co.mrry.mercator.task.Task;

public class KMeansSeedGenerator implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos,
			String[] args) {

		assert args.length == 1;
		int k = Integer.parseInt(args[0]);
		
		DataInputStream[] dataInputs = new DataInputStream[fis.length];
		for (int i = 0; i < dataInputs.length; ++i) {
			dataInputs[i] = new DataInputStream(fis[i]);
		}
		
		assert fos.length == 1;
		DataOutputStream clustersOutput = new DataOutputStream(fos[0]);
		ArrayList<Cluster> chosenClusters = new ArrayList<Cluster>(k);
		int nextClusterId = 0;

		try {

			Text key = new Text();
			VectorWritable value = new VectorWritable();
			Random random = RandomUtils.getRandom();
			
			
			for (int i = 0; i < dataInputs.length; ++i) {
				while (true) {
					try {
						key.readFields(dataInputs[i]);
						value.readFields(dataInputs[i]);
					} catch (EOFException eofe) {
						break;
					}
					
					Cluster cluster = new Cluster(value.get(), nextClusterId++);
					int currentSize = chosenClusters.size();
					if (currentSize < k) {
						chosenClusters.add(cluster);
					} else if (random.nextInt(currentSize + 1) == 0) {
						int indexToRemove = random.nextInt(currentSize);
						chosenClusters.remove(indexToRemove);
						chosenClusters.add(cluster);
					}
					
				}
				dataInputs[i].close();
			}
			
			for (Cluster c : chosenClusters) {
				c.write(clustersOutput);
			}
			clustersOutput.close();
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe);
		}
		
	}

}
