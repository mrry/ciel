package skywriting.examples.skyhout.common;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SortedInputTextOutputReduceDriver<K extends WritableComparable, V extends Writable, C, K2 extends WritableComparable, V2 extends Writable> extends
		SortedInputReduceDriver<K, V, C, K2, V2> {

	private BufferedWriter writer;
	
	public SortedInputTextOutputReduceDriver(SkywritingTaskFileSystem fs, CombinerReducer<K, V, C, K2, V2> combiner, Class<K> inputKeyClass, Class<V> inputValueClass, Class<K2> outputKeyClass, Class<V2> outputValueClass) throws IOException {
		super(fs, combiner, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass);
		this.writer = new BufferedWriter(new OutputStreamWriter(fs.append(new Path("/out/0"))));
	}
	
	@Override
	protected void close() throws IOException {
		this.writer.close();
	}

	@Override
	public void collect(K2 key, V2 value) throws IOException {
		this.writer.write(key + "\t= " + value + "\n");
	}

}
