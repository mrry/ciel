package skywriting.examples.skyhout.common;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

public abstract class SortedInputReduceDriver<K extends WritableComparable, V extends Writable, C, K2 extends WritableComparable, V2 extends Writable> implements OutputCollector<K2, V2> {

	private SequenceFile.Reader[] readers;
	private CombinerReducer<K, V, C, K2, V2> combiner;
	
	private ArrayList<K> keyHeads;
	private K spareKey;
	private ArrayList<V> valueHeads;
	
	public SortedInputReduceDriver(SkywritingTaskFileSystem fs, CombinerReducer<K, V, C, K2, V2> combiner, Class<K> inputKeyClass, Class<V> inputValueClass, Class<K2> outputKeyClass, Class<V2> outputValueClass) throws IOException {
		this.readers = new SequenceFile.Reader[fs.numInputs()];
		this.keyHeads = new ArrayList<K>(readers.length);
		this.valueHeads = new ArrayList<V>(readers.length);

		try {
			this.spareKey = inputKeyClass.newInstance();
			for (int i = 0; i < readers.length; ++i) {
				this.readers[i] = new SequenceFile.Reader(fs, new Path("/in/" + i), fs.getConf());
				this.keyHeads.add(inputKeyClass.newInstance());
				this.valueHeads.add(inputValueClass.newInstance());
			}
		} catch (IllegalAccessException iae) {
			throw new RuntimeException(iae);
		} catch (InstantiationException ie) {
			throw new RuntimeException(ie);
		}
		this.combiner = combiner;
	}
	
	public void runReduce() throws IOException {
		
		for (int i = 0; i < readers.length; ++i) {
			try {
				this.readers[i].next(this.keyHeads.get(i), this.valueHeads.get(i));
			} catch (EOFException eofe) {
				this.readers[i].close();
				this.keyHeads.set(i, null);
				this.valueHeads.set(i, null);
			}
		}
		
		do {
			
			int minHeadIndex = -1;
			K minHeadKey = null;
			for (int i = 0; i < this.readers.length; ++i) {
				K currentHeadKey = this.keyHeads.get(i);
				if (currentHeadKey == null) {
					continue;
				} else if (minHeadKey == null || currentHeadKey.compareTo(minHeadKey) < 0) {
					minHeadIndex = i;
					minHeadKey = currentHeadKey;
				}
			}
			if (minHeadIndex == -1) break;
			
			C currentValue = this.popInitialValueFromHead(minHeadIndex);
			
			for (int i = minHeadIndex + 1; i < this.readers.length; ++i) {
				K currentHeadKey = this.keyHeads.get(i);
				if (currentHeadKey == null) {
					continue;
				} else if (minHeadKey.compareTo(currentHeadKey) == 0) {
					this.combiner.combine(currentValue, this.valueHeads.get(i));
					this.advanceReader(i);
				}
			}
			
			this.combiner.reduce(minHeadKey, currentValue, this);
			
		} while (true);
		
		this.close();
		
	}
	
	protected abstract void close() throws IOException;
	
	private C popInitialValueFromHead(int i) throws IOException {
		
		// Get the head value.
		C headValue = this.combiner.combineInit(this.valueHeads.get(i));
		
		// We use a "spare key" to avoid instantiating too many Writable wrapper objects.
		K temp = this.keyHeads.get(i);
		this.keyHeads.set(i, this.spareKey);
		this.spareKey = temp;
		
		this.advanceReader(i);
		
		return headValue;
		
	}
	
	private void advanceReader(int i) throws IOException {
		try { 
			this.readers[i].next(this.keyHeads.get(i), this.valueHeads.get(i));
		} catch (EOFException eofe) {
			this.readers[i].close();
			this.keyHeads.set(i, null);
			this.valueHeads.set(i, null);
		}
	}

	public abstract void collect(K2 key, V2 value) throws IOException;
	
	
	
}
