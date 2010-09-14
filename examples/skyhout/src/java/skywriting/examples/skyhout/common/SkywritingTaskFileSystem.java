package skywriting.examples.skyhout.common;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class SkywritingTaskFileSystem extends FileSystem {

	private static Path WORKING_DIRECTORY = new Path("/");
	
	private InputStream[] inputs;
	private OutputStream[] outputs;
	
	public SkywritingTaskFileSystem(InputStream[] inputs, OutputStream[] outputs) {
		this.inputs = inputs;
		this.outputs = outputs;
	}
	
	@Override
	public FSDataOutputStream append(Path arg0, int arg1, Progressable arg2)
			throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public FSDataOutputStream create(Path arg0, FsPermission arg1,
			boolean arg2, int arg3, short arg4, long arg5, Progressable arg6)
			throws IOException {
		// TODO Need to implement this one for the writers.	
		throw new NotImplementedException();
	}

	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public FileStatus getFileStatus(Path arg0) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public URI getUri() {
		throw new NotImplementedException();
	}

	@Override
	public Path getWorkingDirectory() {
		return SkywritingTaskFileSystem.WORKING_DIRECTORY;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws FileNotFoundException,
			IOException {
		throw new NotImplementedException();
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		throw new NotImplementedException();
	}

	@Override
	public FSDataInputStream open(Path arg0, int arg1) throws IOException {
		// TODO Implement this one.
		throw new NotImplementedException();
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		return false;
	}

	@Override
	public void setWorkingDirectory(Path arg0) {
		throw new NotImplementedException();
	}

}
