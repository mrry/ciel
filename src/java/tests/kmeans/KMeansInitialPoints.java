package tests.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import uk.co.mrry.mercator.task.Task;

public class KMeansInitialPoints implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			int dimension = Integer.parseInt(args[1]);
			int k = Integer.parseInt(args[0]);
			char mode = args[2].charAt(0);

			DataOutputStream pointsOutput = new DataOutputStream(fos[0]);
			Random rand = new Random();
			
			if (mode == 'u') {
				for (int i = 0; i < k; ++i) {
					for (int j = 0; j < dimension; ++j) {
						pointsOutput.writeDouble((rand.nextDouble() * 100.0) - 50.0);
					}
				}
			} else if (mode == 'g') {
				int numSyntheticCentroids = Integer.parseInt(args[3]);
				double[][] clusterCentroids = new double[numSyntheticCentroids][dimension];
				for (int i = 0; i < numSyntheticCentroids; ++i) {
					for (int j = 0; j < dimension; ++j) {
						pointsOutput.writeDouble((rand.nextDouble() * 100.0) - 50.0);
					}
				}
				
				for (int i = 0; i < k; ++i) {
					int cluster = rand.nextInt(numSyntheticCentroids);
					for (int j = 0; j < dimension; ++j) {
						pointsOutput.writeDouble(clusterCentroids[cluster][j] + 20.0 * rand.nextGaussian());
					}
				}
			} else {
				throw new RuntimeException("Invalid mode specified");
			}

			pointsOutput.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
