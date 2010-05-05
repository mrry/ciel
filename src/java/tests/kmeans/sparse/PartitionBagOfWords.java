package tests.kmeans.sparse;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Random;
import java.util.Scanner;

import uk.co.mrry.mercator.task.Task;

public class PartitionBagOfWords implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			Scanner scanner = new Scanner(fis[0]);
			
			int numPoints = scanner.nextInt();
			int dimension = scanner.nextInt();
			// Number of examples.
			int numExamples = scanner.nextInt();
			
			int currentPartition = 0;
			int currentExamplesInPartition = 0;
			int currentExample = 0;
			int examplesPerPartition = (numExamples / fos.length) + 1;
			
			int currentDocID = -1;
			
			DataOutputStream currentOutput = new DataOutputStream(fos[0]);
			
			while (scanner.hasNextInt()) {
				int docID = scanner.nextInt();
				int wordID = scanner.nextInt();
				int count = scanner.nextInt();
				if (docID != currentDocID && currentExamplesInPartition > examplesPerPartition && currentPartition < (fos.length - 1)) {
					++currentPartition;
					currentExamplesInPartition = 0;
					currentOutput.close();
					currentOutput = new DataOutputStream(fos[currentPartition]);
					currentDocID = docID;
				}
				currentOutput.writeInt(docID);
				currentOutput.writeInt(wordID);
				currentOutput.writeInt(count);
				++currentExamplesInPartition;
			}

			currentOutput.close();
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
			
	}

}
