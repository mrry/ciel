package tests.kmeans.sparse;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedList;
import java.util.List;

import tests.kmeans.sparse.KMeansMap.CentroidEntry;
import uk.co.mrry.mercator.task.Task;

public class KMeansComputeError implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			System.out.println("Invoking ComputeError (stdout)");
			System.err.println("Invoking ComputeError (stderr)");
			
//			DataInputStream oldCentroidsFile = new DataInputStream(fis[0]);
//			DataInputStream newCentroidsFile = new DataInputStream(fis[1]);

			Writer resultWriter = new BufferedWriter(new OutputStreamWriter(fos[0]));
			resultWriter.write(Boolean.toString(true));
			resultWriter.close();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

}
