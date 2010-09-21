package skywriting.examples.skyhout.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IntArrayWritable implements Writable {

	int[] arr;
	
	public IntArrayWritable(int[] arr) {
		this.arr = arr;
	}
	
	public IntArrayWritable() {
		this.arr = null;
	}
	
	public int[] get() {
		return this.arr;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int length = in.readInt();
		this.arr = new int[length];
		for (int i = 0; i < length; ++i) {
			int element = in.readInt();
			this.arr[i] = element;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.arr.length);
		for (int i = 0; i < this.arr.length; ++i) {
			out.writeInt(this.arr[i]);
		}
	}

}
