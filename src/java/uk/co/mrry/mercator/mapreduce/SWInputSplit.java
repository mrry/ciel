/*
 * Copyright (c) 2010 Chris Smowton <chris.smowton@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

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
