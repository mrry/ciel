package skywriting.examples.kmeans;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

public final class KMeansMapperResult implements Serializable {

	public int[] counts;
	public double[][] sums;
	
	public KMeansMapperResult(int k, int numDimensions) {
		this.counts = new int[k];
		this.sums = new double[k][numDimensions];
	}
	
	public final void add(int i, double[] vector) {
		this.counts[i]++;
		for (int j = 0; j < vector.length; ++j) {
			this.sums[i][j] += vector[j];
		}
	}
	
	public final void add(KMeansMapperResult other) {
		for (int i = 0; i < this.counts.length; ++i) {
			this.counts[i] += other.counts[i];
			for (int j = 0; j < this.sums[i].length; ++j) {
				this.sums[i][j] += other.sums[i][j];
			}
		}
	}

	public final double error(double[][] other) {
		double ret = 0.0;
		for (int i = 0; i < this.sums.length; ++i) {
			for (int j = 0; j < this.sums[i].length; ++j) {
				ret += (this.sums[i][j] - other[i][j]) * (this.sums[i][j] - other[i][j]);
			}
		}
		return ret;
	}
	
	public void normalise() {
		for (int i = 0; i < this.sums.length; ++i) {
			for (int j = 0; j < this.sums[i].length; ++j) {
				this.sums[i][j] /= (double) this.counts[i];
			}
			this.counts[i] = 1;
		}
	}
	
}
