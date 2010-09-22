package skywriting.examples.skyhout.pagerank;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;

import skywriting.examples.skyhout.common.Combiner;
import skywriting.examples.skyhout.common.IntArrayWritable;

public class IntListCombiner implements Combiner<IntWritable, List<Integer>, IntWritable, IntArrayWritable> {

	@Override
	public List<Integer> combine(List<Integer> oldValue, IntWritable newValue) {
		oldValue.add(newValue.get());
		return oldValue;
	}

	@Override
	public IntArrayWritable combineFinal(IntWritable key, List<Integer> oldValue)
			throws IOException {
		int[] arr = new int[oldValue.size()];
		int i = 0;
		for (int x : oldValue) {
			arr[i++] = x;
		}
		return new IntArrayWritable(arr);
	}

	@Override
	public List<Integer> combineInit(IntWritable initVal) {
		List<Integer> container = new LinkedList<Integer>();
		container.add(initVal.get());
		return container;
	}

}
