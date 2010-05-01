package uk.co.mrry.mercator.mapreduce;

import java.io.IOException;

import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SWLineRecordWriter<K,V> extends RecordWriter<K,V> {

  @Override
  public void close(TaskAttemptContext arg0) throws IOException,
      InterruptedException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(K arg0, V arg1) throws IOException, InterruptedException {
    // TODO Auto-generated method stub

  }

}
