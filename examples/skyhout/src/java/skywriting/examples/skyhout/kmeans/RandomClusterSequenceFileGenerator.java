package skywriting.examples.skyhout.kmeans;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

public class RandomClusterSequenceFileGenerator {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		
		String prefix = args[0];
		int numFiles = Integer.parseInt(args[1]);
		int vectorsPerFile = Integer.parseInt(args[2]);
		int vectorDimension = Integer.parseInt(args[3]);
		double minValue = Double.parseDouble(args[4]);
		double maxValue = Double.parseDouble(args[5]);

		Text currentKey = new Text();

		Random rand = new Random(1997);
		
		FileSystem fs = new RawLocalFileSystem();
		fs.setConf(new Configuration());
				
		try { 

				SequenceFile.Writer writer = new SequenceFile.Writer(	
						fs, fs.getConf(), new Path(prefix + "_" + 0), Text.class, Cluster.class);
				
				for (int j = 0; j < vectorsPerFile; ++j) {
					
					currentKey.set("CCC" + j);
					
					DenseVector vector = new DenseVector(vectorDimension);
					for (int k = 0; k < vectorDimension; ++k) {
						vector.set(k, (rand.nextDouble() * (maxValue - minValue)) - minValue);
					}
					Cluster cluster = new Cluster(vector, j);
										
					writer.append(currentKey, cluster);
					
				}
				
				writer.close();

		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
			
	}
	
}
