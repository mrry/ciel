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
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.DenseVectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

class VectorMerger {

	public static DenseVector mergeInputs(SkywritingTaskFileSystem fs, int numInputs, int rank) throws IOException {
		
		// Merge the first inputs to make a new vector.
		DenseVector newVector = new DenseVector(new double[rank]);

		IntWritable currentElementIndex = new IntWritable();
		DoubleWritable currentElement = new DoubleWritable();

		for (int i = 0; i < numInputs; ++i) {

			SequenceFile.Reader mapOutputReader = new SequenceFile.Reader(fs, new Path("/in/" + i), fs.getConf());
			while (true) {
				try {
					boolean hasMore = mapOutputReader.next(currentElementIndex, currentElement);
					if (!hasMore) break;
				} catch (EOFException eofe) {
					break;
				}
				
				newVector.set(currentElementIndex.get(), currentElement.get());
			}
			mapOutputReader.close();

		}
		
		return newVector;
		
	}
	
	public static DenseVector readSingleVectorFile(SkywritingTaskFileSystem fs, Path inputPath) throws IOException {
		SequenceFile.Reader inputReader = new SequenceFile.Reader(fs, inputPath, fs.getConf());
		Text dummyKey = new Text();
		DenseVectorWritable vector = new DenseVectorWritable();
		boolean hasContent = inputReader.next(dummyKey, vector);
		assert hasContent;
		return vector;
	}
	
	public static void writeResultFile(SkywritingTaskFileSystem fs, Path outputPath, DenseVector vector) throws IOException {
		// Write out the resulting vector.
		SequenceFile.Writer resultWriter = new SequenceFile.Writer(fs, fs.getConf(), new Path("/out/0"), Text.class, DenseVectorWritable.class);
		resultWriter.append(new Text(), new DenseVectorWritable(vector));
		resultWriter.close();
	}
			
}
