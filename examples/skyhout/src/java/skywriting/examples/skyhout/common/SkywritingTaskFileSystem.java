package skywriting.examples.skyhout.common;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

public class SkywritingTaskFileSystem extends FileSystem {

	private static Path WORKING_DIRECTORY = new Path("/");
	
	private InputStream[] inputs;
	private OutputStream[] outputs;
	private FileSystem.Statistics stats;
	private Configuration configuration;
	
	public SkywritingTaskFileSystem(InputStream[] inputs, OutputStream[] outputs, Configuration conf) {
		this.inputs = inputs;
		this.outputs = outputs;
		this.stats = new FileSystem.Statistics("swbs");
		this.configuration = conf;
	}
	
	public int numOutputs() {
		return this.outputs.length;
	}
	
	public int numInputs() {
		return this.inputs.length;
	}
	
	@Override
	public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
			throws IOException {
		/*
		 *  XXX: We don't perform any sanity-checking on the path name.
		 *  	 We should really verify that the filename looks like "/out/*".
		 *  	 Also, we shouldn't ignore bufferSize or progress.
		 */
		int index = Integer.parseInt(f.getName());
		return new FSDataOutputStream(this.outputs[index], this.stats);
	}

	@Override
	public FSDataOutputStream create(Path f, FsPermission permission,
			boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
			throws IOException {
		/*
		 *  XXX: We don't perform any sanity-checking on the path name.
		 *  	 We should really verify that the filename looks like "/out/*".
		 *       Also, we shouldn't ignore bufferSize, replication, blockSize or progress.
		 */
		int index = Integer.parseInt(f.getName());
		return new FSDataOutputStream(this.outputs[index], this.stats);
	}

	@Override
	public Configuration getConf() {
		return this.configuration;
	}
	
	@Override
	public boolean delete(Path arg0, boolean arg1) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public FileStatus getFileStatus(Path arg0) throws IOException {
		FileStatus ret = new FileStatus(Long.MAX_VALUE, false, 1, Long.MAX_VALUE, 0, arg0);
		return ret;
	}

	@Override
	public URI getUri() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Path getWorkingDirectory() {
		return SkywritingTaskFileSystem.WORKING_DIRECTORY;
	}

	@Override
	public FileStatus[] listStatus(Path arg0) throws FileNotFoundException,
			IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean mkdirs(Path arg0, FsPermission arg1) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public FSDataInputStream open(Path path, int bufferSize) throws IOException {
		/*
		 *  XXX: We don't perform any sanity-checking on the path name.
		 *  	 We should really verify that the filename looks like "/in/*".
		 *       Also, we shouldn't ignore bufferSize.
		 */
		int index = Integer.parseInt(path.getName());
		return new FSDataInputStream(new FakeSeekable(this.inputs[index]));
	}

	@Override
	public boolean rename(Path arg0, Path arg1) throws IOException {
		return false;
	}

	@Override
	public void setWorkingDirectory(Path arg0) {
		throw new UnsupportedOperationException();
	}

	private class FakeSeekable extends InputStream implements Seekable, PositionedReadable {

		private InputStream stream;
		private long pos;
		
		private FakeSeekable(InputStream stream) {
			this.stream = stream;
			this.pos = 0;
		}
		
		@Override
		public long getPos() throws IOException {
			return this.pos;
		}

		@Override
		public void seek(long newPos) throws IOException {
			while (this.pos < newPos) {
				this.read();
			}
		}

		@Override
		public boolean seekToNewSource(long arg0) throws IOException {
			throw new UnsupportedOperationException();
		}

		@Override
		public int read() throws IOException {
			++this.pos;
			return this.stream.read();
		}

		@Override
		public int read(long position, byte[] buffer, int offset, int length)
				throws IOException {
			if (position < this.pos) {
				this.seek(position);
			}
			
			int i;
			for (i = 0; i < length; ++i) {
				int byteRead = this.read();
				if (byteRead == -1)
					break;
				buffer[offset + i] = (byte) byteRead;
			}
			return i;
		}

		@Override
		public void readFully(long position, byte[] buffer) throws IOException {
			this.readFully(position, buffer, 0, buffer.length);
		}

		@Override
		public void readFully(long position, byte[] buffer, int offset, int length)
				throws IOException {
			int bytesRead = this.read(position, buffer, offset, length);
			if (bytesRead < length) {
				throw new EOFException("Attempted to readFully past the end of the file.");
			}
		}
		
	}
	
	
}
