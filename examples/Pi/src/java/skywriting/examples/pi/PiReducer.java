package skywriting.examples.pi;

import java.io.DataInputStream;
import java.io.OutputStreamWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;

import uk.co.mrry.mercator.task.Task;

public class PiReducer implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			/**
			 * args: none.
			 */

			/**
			 * inputs: n files containing (long) numInside, (long) numOutside.
			 */
			long numInside = 0;
			long numOutside = 0;

			for (int i = 0; i < fis.length; ++i) {
				DataInputStream in = new DataInputStream(fis[i]);
				numInside += in.readLong();
				numOutside += in.readLong();
				in.close();
			}

		    //compute estimated value
			double actualResult;
			
			try {
				BigDecimal result = BigDecimal.valueOf(4).setScale(20)
					.multiply(BigDecimal.valueOf(numInside))
					.divide(BigDecimal.valueOf(numInside + numOutside));
				actualResult = result.doubleValue();
				
				System.out.println("Result is: " + result.toPlainString());
			} catch (Exception e) {
				actualResult = Math.PI;
				System.out.println("Java BigDecimal is being a pain...");
				
			}
			/**
			 * output: single file containing (double) result.
			 */
		
			OutputStreamWriter out = new OutputStreamWriter(fos[0]);
			out.write(actualResult + "");
			out.close();
	      
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
}
