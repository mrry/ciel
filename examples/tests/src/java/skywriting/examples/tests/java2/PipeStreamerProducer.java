package skywriting.examples.tests.java2;

import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class PipeStreamerProducer implements FirstClassJavaTask {

	private boolean may_stream;
	private boolean may_pipe;
	private int chunk_size;
	private int n_chunks;
	
	public PipeStreamerProducer(int chunk_size, int n_chunks, boolean may_stream, boolean may_pipe) {
		this.may_stream = may_stream;
		this.may_pipe = may_pipe;
		this.chunk_size = chunk_size;
		this.n_chunks = n_chunks;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	@Override
	public void invoke() throws Exception {
		
		WritableReference out_ref = Ciel.RPC.getOutputFilename(1, this.may_stream, this.may_pipe, false);
		OutputStream out = out_ref.open();
		int bytes_written = 0;
		byte[] out_array = new byte[4096];
		for(int i = 0; i < 4096; i++) {
			out_array[i] = (byte)((i % 32) + 32);
		}
		
		for(int i = 0; i < this.n_chunks; i++) {
			for(int j = 0; j < this.chunk_size / 4096; j++) {
				out.write(out_array);
				bytes_written += 4096;
			}
		}

		out.close();
		
		Ciel.returnPlainString("Wrote " + bytes_written + " bytes");
		
	}

	@Override
	public void setup() {
		;
	}

}
