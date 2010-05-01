package tests.kmeans.sparse;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Random;
import java.util.Scanner;

import uk.co.mrry.mercator.task.Task;

public class GenerateRandomSparse implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			Scanner scanner = new Scanner(fis[0]);
			
			int numClusters = Integer.parseInt(args[0]);
			
			int numPoints = scanner.nextInt();
			int dimension = scanner.nextInt();
			// Number of examples.
			int numExamples = scanner.nextInt();
			
			double prob = ((double) numExamples) / ((double) numPoints);
			Random rand = new Random();
			
			DataOutputStream output = new DataOutputStream(fos[0]);
			
			for (int i = 0; i < numClusters; ++i) {
				for (int j = 0; j < dimension; ++j) {
					if (rand.nextDouble() < prob) {
						output.writeInt(i);
						output.writeInt(j);
						output.writeDouble(1.0);
					}
				}
			}
			
			output.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
			
	}

}
