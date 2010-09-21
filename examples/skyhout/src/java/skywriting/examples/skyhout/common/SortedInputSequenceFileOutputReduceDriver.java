package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class SortedInputSequenceFileOutputReduceDriver<K extends WritableComparable, V extends Writable, C, K2 extends WritableComparable, V2 extends Writable> extends
		SortedInputReduceDriver<K, V, C, K2, V2> {

	private SequenceFile.Writer writer;
	
	public SortedInputSequenceFileOutputReduceDriver(SkywritingTaskFileSystem fs, CombinerReducer<K, V, C, K2, V2> combiner, Class<K> inputKeyClass, Class<V> inputValueClass, Class<K2> outputKeyClass, Class<V2> outputValueClass) throws IOException {
		super(fs, combiner, inputKeyClass, inputValueClass, outputKeyClass, outputValueClass);
		this.writer = new SequenceFile.Writer(fs, fs.getConf(), new Path("/out/0"), outputKeyClass, outputValueClass);
	}

	protected void close() throws IOException {
		this.writer.close();
	}
	
	@Override
	public void collect(K2 key, V2 value) throws IOException {
		this.writer.append(key, value);
	}
	
}
