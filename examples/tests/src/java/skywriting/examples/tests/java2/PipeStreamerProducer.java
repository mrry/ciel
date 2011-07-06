package skywriting.examples.tests.java2;

import java.io.OutputStream;
import java.util.Random;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class PipeStreamerProducer implements FirstClassJavaTask {

	private final boolean may_stream;
	private final boolean may_pipe;
	private final int chunk_size;
	private final long nBytes;
	
	public PipeStreamerProducer(int chunk_size, long nBytes, boolean may_stream, boolean may_pipe) {
		this.may_stream = may_stream;
		this.may_pipe = may_pipe;
		this.chunk_size = chunk_size;
		this.nBytes = nBytes;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	@Override
	public void invoke() throws Exception {
		
		WritableReference out_ref = Ciel.RPC.getOutputFilename(1, this.may_stream, this.may_pipe, false);
		OutputStream out = out_ref.open();
		long bytes_written = 0;
		byte[] out_array = new byte[4096];
		Random random = new Random(0);
		random.nextBytes(out_array);
		
		for (long i = 0; i < this.nBytes; i += 4096) {
		    int bytesToWrite = (i + 4096 > this.nBytes) ? (int) (this.nBytes - i) : 4096;
		    out.write(out_array, 0, bytesToWrite);
		    bytes_written += bytesToWrite;
		}

		out.close();
		
		Ciel.returnPlainString("Wrote " + bytes_written + " bytes");
		
	}

	@Override
	public void setup() {
		;
	}

}
