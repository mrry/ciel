package skywriting.examples.pi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;

public class PiMapper implements Task {

	/** 2-dimensional Halton sequence {H(i)},
	 * where H(i) is a 2-dimensional point and i >= 1 is the index.
	 * Halton sequence is used to generate sample points for Pi estimation. 
	 */
	private static class HaltonSequence {
		/** Bases */
		static final int[] P = {2, 3}; 
		/** Maximum number of digits allowed */
		static final int[] K = {63, 40}; 

		private long index;
		private double[] x;
		private double[][] q;
		private int[][] d;

		/** Initialize to H(startindex),
		 * so the sequence begins with H(startindex+1).
		 */
		HaltonSequence(long startindex) {
			index = startindex;
			x = new double[K.length];
			q = new double[K.length][];
			d = new int[K.length][];
			for(int i = 0; i < K.length; i++) {
				q[i] = new double[K[i]];
				d[i] = new int[K[i]];
			}

			for(int i = 0; i < K.length; i++) {
				long k = index;
				x[i] = 0;

				for(int j = 0; j < K[i]; j++) {
					q[i][j] = (j == 0? 1.0: q[i][j-1])/P[i];
					d[i][j] = (int)(k % P[i]);
					k = (k - d[i][j])/P[i];
					x[i] += d[i][j] * q[i][j];
				}
			}
		}

		/** Compute next point.
		 * Assume the current point is H(index).
		 * Compute H(index+1).
		 * 
		 * @return a 2-dimensional point with coordinates in [0,1)^2
		 */
		double[] nextPoint() {
			index++;
			for(int i = 0; i < K.length; i++) {
				for(int j = 0; j < K[i]; j++) {
					d[i][j]++;
					x[i] += q[i][j];
					if (d[i][j] < P[i]) {
						break;
					}
					d[i][j] = 0;
					x[i] -= (j == 0? 1.0: q[i][j-1]);
				}
			}
			return x;
		}
	}
	
	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		/**
		 * inputs: none.
		 */
		
		/**
		 * args: (int) number of samples, (int) initial offset.
		 */
		long numSamples = Long.parseLong(args[0]);
		long offset = Long.parseLong(args[1]);
		final HaltonSequence haltonsequence = new HaltonSequence(offset);
		long numInside = 0L;
		long numOutside = 0L;

		for (long i = 0; i < numSamples; ++i) {
	        //generate points in a unit square
	        final double[] point = haltonsequence.nextPoint();

	        //count points inside/outside of the inscribed circle of the square
	        final double x = point[0] - 0.5;
	        final double y = point[1] - 0.5;
	        if (x*x + y*y > 0.25) {
	        	numOutside++;
	        } else {
	        	numInside++;
	        }
		}

		/**
		 * output: single file containing (long) numInside, (long) numOutside.
		 */
		try {
			DataOutputStream out = new DataOutputStream(fos[0]);
			out.writeLong(numInside);
			out.writeLong(numOutside);
			out.close();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}

	public static void main(String[] args) {
		int numMappers = Integer.parseInt(args[0]);
		int numSamples = Integer.parseInt(args[1]);
		
		ByteArrayOutputStream[] mapOutputs = new ByteArrayOutputStream[numMappers];
		InputStream[] reduceInputs = new ByteArrayInputStream[numMappers];
		for (int i = 0; i < numMappers; ++i) {
			mapOutputs[i] = new ByteArrayOutputStream();
			new PiMapper().invoke(new InputStream[] {},
					new OutputStream[] { mapOutputs[i] },
					new String[] { Integer.toString(numSamples), Integer.toString(i * numSamples) });
			reduceInputs[i] = new ByteArrayInputStream(mapOutputs[i].toByteArray());
		}
		
		ByteArrayOutputStream reduceOutput = new ByteArrayOutputStream();
		new PiReducer().invoke(reduceInputs, new OutputStream[] { reduceOutput }, new String[] { });
		try {
			System.out.printf("Result is: %f\n", new DataInputStream(new ByteArrayInputStream(reduceOutput.toByteArray())).readDouble());
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
}
