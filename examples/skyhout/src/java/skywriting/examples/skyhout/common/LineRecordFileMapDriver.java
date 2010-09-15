package skywriting.examples.skyhout.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class LineRecordFileMapDriver<K2, V2> extends MapDriver<LongWritable, Text, K2, V2> {

	private BufferedReader reader;
	private int currentLine;
	
	public LineRecordFileMapDriver(SkywritingTaskFileSystem fs, ClosableOutputCollector<K2, V2> outputCollector, Mapper<LongWritable, Text, K2, V2> mapper) throws IOException {
		super(outputCollector, mapper, LongWritable.class, Text.class);
		this.currentLine = 0;
		this.reader = new BufferedReader(new InputStreamReader(fs.open(new Path("/in/0")))); 
	}
	
	public boolean readNextKeyValue(LongWritable key, Text value) throws IOException {
		String line = this.reader.readLine();
		if (line != null) {
			key.set(this.currentLine++);
			value.set(line);
			return true;
		} else {
			return false;
		}
	}
	
}