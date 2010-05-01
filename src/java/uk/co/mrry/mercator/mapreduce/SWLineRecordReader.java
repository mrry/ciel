package uk.co.mrry.mercator.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SWLineRecordReader extends RecordReader<LongWritable, Text> {

  private BufferedReader in = null;
  private long pos = 0;
  private long start = 0;
  private long end = 0;
  private LongWritable key = null;
  private Text value = null;
  
  @Override
  public void close() throws IOException {
    // Close down the input stream
    if (in != null) {
      in.close(); 
    }
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    // Simply return the key stored (can be null!)
    return key;
  }

  @Override
  public Text getCurrentValue() throws IOException, InterruptedException {
    // Simply return the value stored (can be null!)
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // Return some notion of progress - we simply use the position within the split range considered
    if (start == end) return 0.0f;
    else return ((float)pos - (float)start) / ((float)end - (float)start);
  }

  @Override
  public void initialize(InputSplit genericSplit, TaskAttemptContext tac)
      throws IOException, InterruptedException {
    
    // Cast to FileSplit as this is closest to what we're using
    FileSplit split = (FileSplit)genericSplit;
    
    String filePath = split.getPath().toString();
    
    // Open the input file
    FileInputStream fis = new FileInputStream(filePath);
    in = new BufferedReader(new InputStreamReader(fis));
    
    // Set the position within the input split
    pos = fis.getChannel().position();
    start = 0;
    end = fis.getChannel().size();
    
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    // Check if key and value objects exist and create them if not
    if (key == null) {
      key = new LongWritable();
    }
    if (value == null) {
      value = new Text();
    }
    // Set the key to the current position within the input split
    key.set(pos);
    
    // Read from the file, store result and record how many bytes were read
    String line = in.readLine();
    value.set(line);
    int bytesRead = line.length();
    
    if (bytesRead == 0) {
      // If nothing was read, invalidate key and value
      key = null;
      value = null;
    }
    return (bytesRead != 0);
  }

}
