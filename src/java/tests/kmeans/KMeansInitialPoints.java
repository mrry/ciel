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
			
			Random rand = new Random();
			DataOutputStream pointsOutput = new DataOutputStream(fos[0]);
			for (int i = 0; i < k; ++i) {
				for (int j = 0; j < dimension; ++j) {
					pointsOutput.writeDouble((rand.nextDouble() * 100.0) - 50.0);
				}
			}

			pointsOutput.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
