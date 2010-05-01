package tests.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;

public class KMeansMap implements Task {

	public static double distance(double[] x, double[] y, int dimension) {
		double ret = 0.0;
		for (int j = 0; j < dimension; ++j) {
			ret += (y[j] - x[j]) * (y[j] - x[j]);
		}
		return Math.sqrt(ret);
	}
	
	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			int dimension = Integer.parseInt(args[1]);
			int k = Integer.parseInt(args[0]);
			double[][] centroids = new double[k][dimension];
			int[] centroidCounts = new int[k];
			double[][] centroidTotals = new double[k][dimension];
			DataInputStream pointsInput = new DataInputStream(fis[0]);
			DataInputStream centroidsInput = new DataInputStream(fis[1]);
			for (int i = 0; i < k; ++i) {
				for (int j = 0; j < dimension; ++j) {
					centroids[i][j] = centroidsInput.readDouble();
				}
			}
			
			double[] currentPoint = new double[dimension];
			
			try { 
				while (true) {
					for (int j = 0; j < dimension; ++j) {
						currentPoint[j] = pointsInput.readDouble();
					}
					int bestCentroidIndex = -1;
					double bestCentroidDistance = Double.MAX_VALUE;
					for (int i = 0; i < k; ++i) {
						double currentCentroidDistance = distance(currentPoint, centroids[i], dimension);
						if (currentCentroidDistance < bestCentroidDistance) {
							bestCentroidDistance = currentCentroidDistance;
							bestCentroidIndex = i;
						}
					}
					for (int j = 0; j < dimension; ++j) {
						centroidTotals[bestCentroidIndex][j] += currentPoint[j];
					}
				}
			} catch (EOFException eofe) {
				;
			}
			
			DataOutputStream totalsCentroidsOutput = new DataOutputStream(fos[0]);
			for (int i = 0; i < k; ++i) {
				totalsCentroidsOutput.writeInt(centroidCounts[i]);
			}
			for (int i = 0; i < k; ++i) {
				for (int j = 0; j < dimension; ++j) {
					totalsCentroidsOutput.writeDouble(centroidTotals[i][j]);
				}
			}

			totalsCentroidsOutput.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
