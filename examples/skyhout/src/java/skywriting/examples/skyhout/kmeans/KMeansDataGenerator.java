package skywriting.examples.skyhout.kmeans;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.VectorWritable;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansDataGenerator implements FirstClassJavaTask {

	private final int numVectors;
	private final int numDimensions;
	private final int part;
	
	public KMeansDataGenerator(int numVectors, int numDimensions, int part) {
		this.numVectors = numVectors;
		this.numDimensions = numDimensions;
		this.part = part;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		double minValue = -1000000.0;
		double maxValue = 1000000.0;
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		
		DataOutputStream dos = new DataOutputStream(out.open());
		
		Configuration conf = new Configuration();
		conf.setClassLoader(Ciel.CLASSLOADER);
		
		FileSystem fs = new SkywritingTaskFileSystem(new InputStream[0], new OutputStream[] { dos }, conf);
		
		Text currentKey = new Text();
		VectorWritable currentVector = new VectorWritable();
		
		Random rand = new Random(this.part);
		
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, fs.getConf(), new Path("/out/0"), Text.class, VectorWritable.class);
		
		for (int j = 0; j < this.numVectors; ++j) {
			
			currentKey.set("I" + this.part + ":" + j);
			
			DenseVector vector = new DenseVector(this.numDimensions);
			for (int k = 0; k < this.numDimensions; ++k) {
				vector.set(k, (rand.nextDouble() * (maxValue - minValue)) - minValue);
			}
			currentVector.set(vector);
			
			//System.out.println(j + ": length = " + vector.size());
			
			writer.append(currentKey, currentVector);
			
		}

		writer.close();

		//dos.close();
		//Ciel.RPC.closeOutput(0);
	}

	@Override
	public void setup() {

	}

}
