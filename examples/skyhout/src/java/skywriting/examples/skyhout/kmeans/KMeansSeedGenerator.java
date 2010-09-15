package skywriting.examples.skyhout.kmeans;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.VectorWritable;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class KMeansSeedGenerator implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos,
			String[] args) {

		try {
		
			Configuration conf = new Configuration();

			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
			new WritableSerialization();

			SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);

			
			assert args.length == 1;
			int k = Integer.parseInt(args[0]);
			
			assert fos.length == 1;
			ArrayList<Text> chosenTexts = new ArrayList<Text>(k);
			ArrayList<Cluster> chosenClusters = new ArrayList<Cluster>(k);
			

			Text key = new Text();
			Random random = RandomUtils.getRandom();
			
			int nextClusterId = 0;
			
			for (int i = 0; i < fis.length; ++i) {
				System.out.println("Reading file " + 1);
				SequenceFile.Reader inputReader = new SequenceFile.Reader(fs, new Path("/in/" + i), conf);
				VectorWritable value = new VectorWritable();
				
				
				while (true) {
				
					try { 
						boolean more = inputReader.next(key, value);
						if (!more) break;
					} catch (EOFException eofe) {
						break;
					}
				
					Cluster cluster = new Cluster(value.get(), nextClusterId++);
					int currentSize = chosenClusters.size();
					if (currentSize < k) {
						chosenTexts.add(new Text(key.toString()));
						chosenClusters.add(cluster);
					} else if (random.nextInt(currentSize + 1) == 0) {
						int indexToRemove = random.nextInt(currentSize);
						chosenTexts.remove(indexToRemove);
						chosenClusters.remove(indexToRemove);
						chosenTexts.add(new Text(key.toString()));
						chosenClusters.add(cluster);
					}
				}
				
				inputReader.close();
			}
			
			SequenceFile.Writer clustersOutput = new SequenceFile.Writer(fs, conf, new Path("/out/0"), Text.class, Cluster.class);
			for (int i = 0; i < k; ++i) {
				System.out.println("Output cluster: " + chosenClusters.get(i).getIdentifier());
				clustersOutput.append(chosenTexts.get(i), chosenClusters.get(i));
			}
			clustersOutput.close();
			
		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new RuntimeException(ioe);
		}
		
	}

}
