package uk.co.mrry.mercator.mapreduce;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.ReflectionUtils;

import uk.co.mrry.mercator.task.Task;

public class SWReduceEntryPoint implements Task {

  @SuppressWarnings("unchecked")
  @Override
  public void invoke(FileInputStream[] fis, FileOutputStream[] fos, String[] args) {
    // This method is used to kick off a Hadoop Mapreduce job running on Skywriting
    
    /* We assume that args supplies the necessary class names in the following form:
     * 1) Mapper class name
     * 2) Reducer class name
     * 3) Partitioner class name
     * 4) Input record reader class name
     */
    if(args.length < 4) 
      System.out.println("[Skywriting] Insufficient arguments passed to SWReduceEntryPoint");
    
    String mapperClassName = args[0];
    String reducerClassName = args[1];
    String partitionerClassName = args[2];
    String inputRecordReaderClassName = args[3];
    
    try {
      
      // make a reducer
      //ReflectionUtils.newInstance(Class.forName(reducerClassName), null);
      
      // TODO logic to run the reducer
      
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    
  }

}
