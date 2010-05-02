package tests.kmeans;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import uk.co.mrry.mercator.task.Task;

public class KMeansComputeError implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			System.out.println("Invoking ComputeError (stdout)");
			System.err.println("Invoking ComputeError (stderr)");
			
			DataInputStream oldCentroidsFile = new DataInputStream(fis[0]);
			DataInputStream newCentroidsFile = new DataInputStream(fis[1]);
			int dimension = Integer.parseInt(args[0]);
			double epsilon = Double.parseDouble(args[1]);
	
			double[] currentOldCentroid = new double[dimension];
			double[] currentNewCentroid = new double[dimension];
			
			boolean noPointsMoved = true;
			
			try {
				while (noPointsMoved) {
					for (int j = 0; j < dimension; ++j) {
						currentOldCentroid[j] = oldCentroidsFile.readDouble();
					}
					for (int j = 0; j < dimension; ++j) {
						currentNewCentroid[j] = newCentroidsFile.readDouble();
					}
					double currentMove = KMeansMap.distance(currentOldCentroid, currentNewCentroid, dimension);
					System.out.println("Current move: " + currentMove);
					if (currentMove > epsilon) {
						noPointsMoved = false;
					}
				}
			} catch (EOFException eofe) {
				;
			}
			
			System.out.println("Computed Error result: " + noPointsMoved);
			
			Writer resultWriter = new BufferedWriter(new OutputStreamWriter(fos[0]));
			resultWriter.write(Boolean.toString(noPointsMoved));
			resultWriter.close();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

}
