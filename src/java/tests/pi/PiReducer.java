package tests.pi;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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
			BigDecimal result = BigDecimal.valueOf(4).setScale(20)
				.multiply(BigDecimal.valueOf(numInside))
				.divide(BigDecimal.valueOf(numInside + numOutside));

			System.out.println("Result is: " + result.toPlainString());
			
			/**
			 * output: single file containing (double) result.
			 */
			DataOutputStream out = new DataOutputStream(fos[0]);
			out.writeDouble(result.doubleValue());
			out.close();
	      
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
}
