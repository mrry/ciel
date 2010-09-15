package skywriting.examples.skyhout.linalg;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.mahout.math.VectorWritable;

import skywriting.examples.skyhout.common.PartialHashOutputCollector;
import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class MatrixVectorMultiplyTask implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

		try {
		
			Configuration conf = new Configuration();
			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);

			SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);

			// Input[0] is the matrix chunk; [1] is the vector.
			assert fs.numInputs() == 2;
			
			// Output[0] is the vector chunk.
			assert fs.numOutputs() == 1;
			
			// Read in the vector.
			SequenceFile.Reader vectorReader = new SequenceFile.Reader(fs, new Path("/in/1"), conf);
			Text dummyKey = new Text();
			VectorWritable vector = new VectorWritable();
			vectorReader.next(dummyKey, vector);
			vectorReader.close();
			
			// Iterate over matrix chunk rows.
			SequenceFile.Reader matrixReader = new SequenceFile.Reader(fs, new Path("/in/0"), conf);
			IntWritable currentRowIndex = new IntWritable();
			VectorWritable currentRow = new VectorWritable();
			
			SequenceFile.Writer output = new SequenceFile.Writer(fs, conf, new Path("/out/0"), IntWritable.class, DoubleWritable.class);

			DoubleWritable dotProduct = new DoubleWritable();
			while (true) {
				try {
					boolean hasMore = matrixReader.next(currentRowIndex, currentRow);
					if (!hasMore) break;
				} catch (EOFException eofe) {
					break;
				}
				
				dotProduct.set(currentRow.get().dot(vector.get()));
				output.append(currentRowIndex, dotProduct);
			}
			output.close();
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

}
