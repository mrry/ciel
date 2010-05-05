package tests.kmeans.sparse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Map.Entry;

import uk.co.mrry.mercator.task.Task;

public class KMeansReduce implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			
			int k = Integer.parseInt(args[0]);
			int[] centroidCounts = new int[k];
			ArrayList<TreeMap<Integer, Double>> outCentroids = new ArrayList<TreeMap<Integer,Double>>(k);
			for (int i = 0; i < k; ++i) {
				outCentroids.add(new TreeMap<Integer, Double>());
			}
			for (int r = 0; r < fis.length; ++r) {
				DataInputStream totalsCentroidsInput = new DataInputStream(fis[r]);
				for (int i = 0; i < k; ++i) {
					centroidCounts[i] += totalsCentroidsInput.readInt();
				}
				try {
					for (int i = 0; i < k; ++i) {
						int centroidID = totalsCentroidsInput.readInt();
						int wordID = totalsCentroidsInput.readInt();
						double weight = totalsCentroidsInput.readDouble();
						TreeMap<Integer, Double> outCentroid = outCentroids.get(centroidID);
						if (outCentroid.containsKey(wordID)) {
							double previousWeight = outCentroid.get(wordID);
							outCentroid.put(wordID, previousWeight + weight);
						} else {
							outCentroid.put(wordID, weight);
						}
					}
				} catch (EOFException eofe) {
					;
				}
				totalsCentroidsInput.close();
			}
			DataOutputStream centroidsOutput = new DataOutputStream(fos[0]);
			for (int i = 0; i < k; ++i) {
				for (Entry<Integer, Double> e : outCentroids.get(i).entrySet()) {
					centroidsOutput.writeInt(i);
					centroidsOutput.writeInt(e.getKey());
					centroidsOutput.writeDouble(e.getValue());
				}
			}
			centroidsOutput.close();
						
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
