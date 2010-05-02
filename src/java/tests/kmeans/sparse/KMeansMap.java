package tests.kmeans.sparse;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.Map.Entry;

import uk.co.mrry.mercator.task.Task;

public class KMeansMap implements Task {

	public static double distance(List<int[]> x, List<CentroidEntry> c, double cLength) {
		Iterator<CentroidEntry> cIter = c.iterator();
		CentroidEntry currentCentroidEntry = cIter.next();
		double currentDistance = cLength;
		for (int[] examplePoint : x) {
			while (currentCentroidEntry.wordID < examplePoint[0] && cIter.hasNext())
				currentCentroidEntry = cIter.next();
			if (examplePoint[0] == currentCentroidEntry.wordID) {
				currentDistance += (((double) examplePoint[1]) - currentCentroidEntry.weight) * (((double) examplePoint[1]) - currentCentroidEntry.weight) - (currentCentroidEntry.weight * currentCentroidEntry.weight);
			} else if (examplePoint[0] < currentCentroidEntry.wordID) {
				currentDistance += ((double) examplePoint[1]) * ((double) examplePoint[1]);
			}
		}
		return currentDistance;
	}
	
	public static void updateCentroidTotals(TreeMap<Integer, Double> outCentroid, List<int[]> x) {
		for (int[] examplePoint : x) {
			if (outCentroid.containsKey(examplePoint[0])) {
				double currentWeight = outCentroid.get(examplePoint[0]);
				outCentroid.put(examplePoint[0], currentWeight + examplePoint[1]);
			} else {
				outCentroid.put(examplePoint[0], (double)examplePoint[1]);
			}
		}
	}
	
	static final class CentroidEntry {
		int wordID;
		double weight;
		public CentroidEntry (int wordID, double weight) {
			this.wordID = wordID;
			this.weight = weight;
		}
	}
		
	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			int k = Integer.parseInt(args[0]);

			int[] centroidCounts = new int[k];
			double[] centroidLengths = new double[k];
			ArrayList<List<CentroidEntry>> centroids = new ArrayList<List<CentroidEntry>>(k);
			DataInputStream centroidsInput = new DataInputStream(fis[1]);
			int currentCentroidID = 0;
			List<CentroidEntry> currentCentroidVector = new LinkedList<CentroidEntry>();
			try { 
				while (true) {
					int centroidID = centroidsInput.readInt();
					if (centroidID != currentCentroidID) {
						centroids.add(currentCentroidVector);
						currentCentroidVector = new LinkedList<CentroidEntry>();
						currentCentroidID = centroidID;
					}
					CentroidEntry entry = new CentroidEntry(centroidsInput.readInt(), centroidsInput.readDouble());
					currentCentroidVector.add(entry);
					centroidLengths[centroidID] += entry.weight * entry.weight;
				}
			} catch (EOFException eofe) {
				;
			}
			centroids.add(currentCentroidVector);

			ArrayList<TreeMap<Integer, Double>> outCentroids = new ArrayList<TreeMap<Integer, Double>>(k);
			for (int i = 0; i < k; ++i) {
				outCentroids.add(new TreeMap<Integer, Double>());
			}
			
			DataInputStream examplesInput = new DataInputStream(fis[0]);
			
			int currentDocID = 0;
			List<int[]> currentDocVector = new LinkedList<int[]>();
			
			try {

				do {
					int docID = examplesInput.readInt();
					if (docID != currentDocID) {
						// Update count and total vector for closest centroid.
						int bestCentroid = 0;
						double bestCentroidDistance = distance(currentDocVector, centroids.get(0), centroidLengths[0]);
						for (int i = 1; i < k; ++i) {
							double currentCentroidDistance = distance(currentDocVector, centroids.get(i), centroidLengths[i]);
							if (currentCentroidDistance < bestCentroidDistance) {
								bestCentroid = i;
								bestCentroidDistance = currentCentroidDistance;
							}
						}
						
						++centroidCounts[bestCentroid];
						updateCentroidTotals(outCentroids.get(bestCentroid), currentDocVector);
						
					}
					int[] arr = new int[2];
					arr[0] = examplesInput.readInt();
					arr[1] = examplesInput.readInt();
					currentDocVector.add(arr);
				} while (true);
			} catch (EOFException eofe) {
				;
			}

			// Update count and total vector for closest centroid to last doc.
			int bestCentroid = 0;
			double bestCentroidDistance = distance(currentDocVector, centroids.get(0), centroidLengths[0]);
			for (int i = 1; i < k; ++i) {
				double currentCentroidDistance = distance(currentDocVector, centroids.get(i), centroidLengths[i]);
				if (currentCentroidDistance < bestCentroidDistance) {
					bestCentroid = i;
					bestCentroidDistance = currentCentroidDistance;
				}
			}
			
			DataOutputStream totalsCentroidsOutput = new DataOutputStream(fos[0]);
			for (int i = 0; i < k; ++i) {
				totalsCentroidsOutput.writeInt(centroidCounts[i]);
			}
			for (int i = 0; i < k; ++i) {
				for (Entry<Integer, Double> e : outCentroids.get(i).entrySet()) {
					totalsCentroidsOutput.writeInt(i);
					totalsCentroidsOutput.writeInt(e.getKey());
					totalsCentroidsOutput.writeDouble(e.getValue());
				}
			}
			totalsCentroidsOutput.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}
