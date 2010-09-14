package skywriting.examples.skyhout.input;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

import org.apache.hadoop.io.Text;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.DenseVectorWritable;

import uk.co.mrry.mercator.task.Task;

public class VectorInputParserTask implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		
		try {
	
			DataOutputStream[] outputs = new DataOutputStream[fos.length];
			for (int i = 0; i < outputs.length; ++i) {
				outputs[i] = new DataOutputStream(fos[i]);
			}
			
			int currentVector = 0;
			
			for (int i = 0; i < fis.length; ++i) {
				
				BufferedReader lineReader = new BufferedReader(new InputStreamReader(fis[i]));
				
				String line;
				while ((line = lineReader.readLine()) != null) {
					String[] fields = line.split("\\s+");
					DenseVectorWritable vector = new DenseVectorWritable(new DenseVector(new double[fields.length]));
					for (int j = 0; j < fields.length; ++j) { 
						vector.set(j, Double.parseDouble(fields[j]));
					}
					Text.writeString(outputs[currentVector % outputs.length], "c" + currentVector);
					vector.write(outputs[(currentVector++) % outputs.length]);
				}
			}
			
			for (int i = 0; i < outputs.length; ++i) {
				outputs[i].close();
			}
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		
	}

}
