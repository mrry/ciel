package skywriting.examples.skyhout.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;

import uk.co.mrry.mercator.task.JarTaskLoader;
import uk.co.mrry.mercator.task.Task;

public abstract class SkyhoutTask implements Task {

	@Override
	public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {
		try {
			
			Configuration conf = new Configuration();
			conf.setClassLoader(JarTaskLoader.CLASSLOADER);
			conf.setClass("io.serializations", WritableSerialization.class, Serialization.class);
			SkywritingTaskFileSystem fs = new SkywritingTaskFileSystem(fis, fos, conf);
			this.invoke(fs, args);
			
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	public abstract void invoke(SkywritingTaskFileSystem fs, String[] args) throws IOException;
	
}
