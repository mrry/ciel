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
import org.apache.mahout.math.function.PlusMult;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class ConjugateGradientReduceTask implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

		try {

			
			Configuration conf = new Configuration();
			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
			SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);
			
			assert args.length == 2;
			
			// args[0] is the rank of the vector.
			int rank = Integer.parseInt(args[0]);
		
			// args[1] is the "first time" flag.
			boolean firstRound = Boolean.parseBoolean(args[1]);
			
			// First round,       m + 1 inputs: [Ap], b.
			// Subsequent rounds, m + 3 inputs: [Ap], x, r and p.
			
			// args[2] is epsilon.
			double epsilon = Double.parseDouble(args[2]);
			
			DenseVector aTimesOldP; 
			DenseVector oldX; 
			DenseVector oldR; 
			DenseVector oldP; 
			
			if (firstRound) {
				aTimesOldP = VectorMerger.mergeInputs(fs, fs.numInputs() - 1, rank);
				oldX = new DenseVector(new double[rank]);
				oldR = VectorMerger.readSingleVectorFile(fs, new Path("/in/" + (fis.length - 1)));
				oldP = oldR.clone();
			} else {
				aTimesOldP = VectorMerger.mergeInputs(fs, fs.numInputs() - 3, rank);
				oldX = VectorMerger.readSingleVectorFile(fs, new Path("/in/" + (fis.length - 3)));
				oldR = VectorMerger.readSingleVectorFile(fs, new Path("/in/" + (fis.length - 2)));
				oldP = VectorMerger.readSingleVectorFile(fs, new Path("/in/" + (fis.length - 1)));
			}

			// Computational phase.
			double oldRdotOldR = oldR.dotSelf();
			double oldPdotAtimesOldP = oldP.dot(aTimesOldP);
			double alpha = oldRdotOldR / oldPdotAtimesOldP;
			DenseVector newX = (DenseVector) oldX.assign(oldP, new PlusMult(alpha));
			DenseVector newR = (DenseVector) oldR.assign(oldP, new PlusMult(-alpha));
			boolean converged = newR.norm(2.0) < epsilon;
			
			// 4 outputs: converged?, newX, newR, newP. We don't write newR or newP if converged.
			Writer convergedOutput = new OutputStreamWriter(fos[0]);
			convergedOutput.write(Boolean.toString(converged));
			VectorMerger.writeResultFile(fs, new Path("/out/1"), newX);
			
			if (!converged) {
				double beta = oldRdotOldR / newR.dotSelf();
				DenseVector newP = (DenseVector) newR.assign(oldP, new PlusMult(beta));
				VectorMerger.writeResultFile(fs, new Path("/out/2"), newR);
				VectorMerger.writeResultFile(fs, new Path("/out/3"), newP);
			}
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

}

		
