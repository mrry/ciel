package skywriting.examples.skyhout.input;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.DenseVectorWritable;
import org.apache.mahout.math.VectorWritable;

import skywriting.examples.skyhout.common.SkywritingTaskFileSystem;
import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public class VectorInputParserTask implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		
		try {
	
			Configuration conf = new Configuration();

			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
			new WritableSerialization();

			SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);

			
			
			SequenceFile.Writer[] writers = new SequenceFile.Writer[fos.length];
			for (int i = 0; i < fos.length; ++i) {
				writers[i] = new SequenceFile.Writer(fs, conf, new Path("/in/" + i), Text.class, VectorWritable.class);
			}
			
			int currentVector = 0;
			
			for (int i = 0; i < fis.length; ++i) {
				
				BufferedReader lineReader = new BufferedReader(new InputStreamReader(fis[i]));
				
				String line;
				while ((line = lineReader.readLine()) != null) {
					String[] fields = line.split("\\s+");
					DenseVector dv = new DenseVector(new double[fields.length]);
					VectorWritable vector = new VectorWritable(dv);
					for (int j = 0; j < fields.length; ++j) { 
						dv.set(j, Double.parseDouble(fields[j]));
					}
					writers[currentVector % writers.length].append(new Text("c" + currentVector), vector);
					++currentVector;
				}
			}
			
			for (int i = 0; i < writers.length; ++i) {
				writers[i].close();
			}
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		
	}

}
