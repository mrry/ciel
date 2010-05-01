package uk.co.mrry.mercator.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;

public class SWInputSplit extends InputSplit {

  private FileInputStream stream;
  
  
  public SWInputSplit(FileInputStream fis) {
    stream = fis;
  }
  
  @Override
  public long getLength() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void readFields(DataInput arg0) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(DataOutput arg0) throws IOException {
    // TODO Auto-generated method stub

  }
  
  
  public FileInputStream getStream() {
    return stream;
  }
  
  
  public void setStream(FileInputStream fis) {
    stream = fis;
  }

}
