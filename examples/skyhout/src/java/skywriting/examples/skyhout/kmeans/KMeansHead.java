package skywriting.examples.skyhout.kmeans;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class KMeansHead implements FirstClassJavaTask {

	private final Reference dataPartitionRef;
	private final int k;
	private final int numDimensions;
	
	public KMeansHead(Reference dataPartitionRef, int k, int numDimensions) {
		this.dataPartitionRef = dataPartitionRef;
		this.k = k;
		this.numDimensions = numDimensions;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[] { this.dataPartitionRef };
	}

	@Override
	public void invoke() throws Exception {
		DataInputStream dis = new DataInputStream(new FileInputStream(Ciel.RPC.getFilenameForReference(this.dataPartitionRef)));
		
		WritableReference out = Ciel.RPC.getOutputFilename(0);
		DataOutputStream dos = new DataOutputStream(out.open());

		Configuration conf = new Configuration();
		conf.setClassLoader(Ciel.CLASSLOADER);
		
		SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(new InputStream[] { dis }, new OutputStream[] { dos }, conf);

		SequenceFile.Reader mapReader = new SequenceFile.Reader(fs, new Path("/in/0"), fs.getConf());
		SequenceFile.Writer writer = new SequenceFile.Writer(	
				fs, fs.getConf(), new Path("/out/0"), Text.class, Cluster.class);
		
		Text currentID = new Text();
		Text currentKey = new Text();
		VectorWritable currentVector = mapReader.getValueClass().asSubclass(VectorWritable.class).newInstance(); 

		for (int i = 0; i < this.k; ++i) {
			mapReader.next(currentID, currentVector);
			currentKey.set("CCC" + i);
			Cluster cluster = new Cluster(currentVector.get(), i);
			writer.append(currentKey, cluster);
		}
		
		writer.close();
		//Ciel.RPC.closeOutput(0);
		
		
	}

	@Override
	public void setup() {
		// TODO Auto-generated method stub

	}

}
