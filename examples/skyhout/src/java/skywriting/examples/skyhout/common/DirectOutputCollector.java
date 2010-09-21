package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class DirectOutputCollector<K, V> implements ClosableOutputCollector<K, V> {

	private SequenceFile.Writer writer;
	
	public DirectOutputCollector(SkywritingTaskFileSystem fs, Class<K> keyClass, Class<V> valueClass) throws IOException {
		this.writer = new SequenceFile.Writer(fs, fs.getConf(), new Path("/out/0"), keyClass, valueClass);
	}
	
	@Override
	public void collect(K key, V value) throws IOException {
		this.writer.append(key, value);
	}

	@Override
	public void close() throws IOException {
		this.writer.close();
	}

	
	
}
