package skywriting.examples.skyhout.common;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class ZipDriver<K extends Writable, V1 extends Writable, V2 extends Writable, KOUT extends Writable, VOUT extends Writable> {

	private SequenceFile.Reader reader1, reader2;
	
	private final Zipper<K, V1, V2, KOUT, VOUT> zipper;
	private final ClosableOutputCollector<KOUT, VOUT> output;
	
	private final K currentKey1;
	private final K currentKey2;
	private final V1 currentValue1;
	private final V2 currentValue2;
	
	public ZipDriver(SkywritingTaskFileSystem fs, Zipper<K, V1, V2, KOUT, VOUT> zipper, ClosableOutputCollector<KOUT, VOUT> outputCollector, Class<K> inputKeyClass, Class<V1> inputValue1Class, Class<V2> inputValue2Class) throws IOException {
		
		this.zipper = zipper;
		this.output = outputCollector;
		
		this.reader1 = new SequenceFile.Reader(fs, new Path("/in/0"), fs.getConf());
		this.reader2 = new SequenceFile.Reader(fs, new Path("/in/1"), fs.getConf());
		
		try {
			this.currentKey1 = inputKeyClass.newInstance();
			this.currentKey2 = inputKeyClass.newInstance();
			this.currentValue1 = inputValue1Class.newInstance();
			this.currentValue2 = inputValue2Class.newInstance();
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		} catch (InstantiationException ie) {
			throw new RuntimeException(ie);
		}
		
	}

	public void runZip() throws IOException {
		while (SequenceFileUtils.read(this.reader1, this.currentKey1, this.currentValue1) && SequenceFileUtils.read(this.reader2, this.currentKey2, this.currentValue2)) {
			this.zipper.zip(this.currentKey1, this.currentValue1, this.currentValue2, this.output);
		}
		this.reader1.close();
		this.reader2.close();
		this.output.close();
	}
	
}
