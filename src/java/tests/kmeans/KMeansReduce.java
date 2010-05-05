package tests.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;

public class KMeansReduce implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			int dimension = Integer.parseInt(args[1]);
			int k = Integer.parseInt(args[0]);
			
			int[] centroidCounts = new int[k];
			double[][] centroidTotals = new double[k][dimension];
			
			for (int r = 0; r < fis.length; ++r) {
				DataInputStream totalsCentroidsInput = new DataInputStream(fis[r]);
				for (int i = 0; i < k; ++i) {
					centroidCounts[i] += totalsCentroidsInput.readInt();
				}
				for (int i = 0; i < k; ++i) {
					for (int j = 0; j < dimension; ++j) {
						centroidTotals[i][j] += totalsCentroidsInput.readDouble();
					}
				}
				totalsCentroidsInput.close();
			}

			DataOutputStream centroidsOutput = new DataOutputStream(fos[0]);
			for (int i = 0; i < k; ++i) {
				if (centroidCounts[i] > 0) { 
					for (int j = 0; j < dimension; ++j) {
						centroidsOutput.writeDouble(centroidTotals[i][j] / centroidCounts[i]);
					}
				} else {
					for (int j = 0; j < dimension; ++j) {
						centroidsOutput.writeDouble(0.0);
					}
				}
			}
			centroidsOutput.close();
						
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
