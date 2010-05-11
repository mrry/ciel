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

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SWLineRecordWriter<K extends Writable, V extends Writable> 
  extends RecordWriter<K,V> {

  private BufferedWriter bufOutput;
  
  private static final String newline;
  static {
    newline = "\n";
  }
  
  private final String keyValueSeparator;
  
  public SWLineRecordWriter(FileOutputStream output, String kvSeparator) {
    // TODO Auto-generated constructor stub
    bufOutput = new BufferedWriter(new OutputStreamWriter(output));
    keyValueSeparator = kvSeparator;
  }
  
  public SWLineRecordWriter(FileOutputStream output) {
    this(output, "\t");
  }
  
  @Override
  public void close(TaskAttemptContext context) throws IOException,
      InterruptedException {
    
    // Close down the output stream
    bufOutput.close();
  }

  @Override
  public void write(K key, V value) throws IOException, InterruptedException {

    boolean nullKey = key == null || key instanceof NullWritable;
    boolean nullValue = value == null || value instanceof NullWritable;
    if (nullKey && nullValue) {
      return;
    }
    if (!nullKey) {
      writeObject(key);
    }
    if (!(nullKey || nullValue)) {
      bufOutput.write(keyValueSeparator);
    }
    if (!nullValue) {
      writeObject(value);
    }
    bufOutput.write(newline);
  }

  private void writeObject(Object o) throws IOException {
    if (o instanceof Text) {
      Text to = (Text) o;
      bufOutput.write(to.toString(), 0, to.getLength());
    } else {
      bufOutput.write(o.toString());
    }
  }
  
}
