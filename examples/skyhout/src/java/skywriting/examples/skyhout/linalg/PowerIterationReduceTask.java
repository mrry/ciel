package skywriting.examples.skyhout.linalg;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.mahout.math.DenseVector;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class PowerIterationReduceTask implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

		try {
			
			Configuration conf = new Configuration();
			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
			SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);
			
			assert args.length == 1;
			
			// args[0] is epsilon.
			double epsilon = Double.parseDouble(args[2]);
			
			// All rounds, m + 1 inputs: [Ax], x.
			DenseVector aTimesOldX; 
			DenseVector oldX;
		
			oldX = VectorMerger.readSingleVectorFile(fs, new Path("/in/" + (fis.length - 1)));
			aTimesOldX = VectorMerger.mergeInputs(fs, fs.numInputs() - 1, oldX.size());
			
			// Computational phase.
			DenseVector normalizedAtimesOldX = (DenseVector) aTimesOldX.normalize(1.0);
			DenseVector residue = (DenseVector) oldX.minus(aTimesOldX);
			
			boolean converged = residue.norm(2.0) < epsilon; 
			
			// 2 outputs: converged?, normalizedAtimesOldX.
			Writer convergedOutput = new OutputStreamWriter(fos[0]);
			convergedOutput.write(Boolean.toString(converged));
			convergedOutput.close();
			
			VectorMerger.writeResultFile(fs, new Path("/out/1"), normalizedAtimesOldX);
				
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		
	}

}
