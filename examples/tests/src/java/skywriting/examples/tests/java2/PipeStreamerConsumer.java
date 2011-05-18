package skywriting.examples.tests.java2;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class PipeStreamerConsumer implements FirstClassJavaTask {

	private boolean may_stream;
	private int chunk_size;
	private boolean sole_consumer;
	private boolean must_block;
	private boolean do_log;
	private Reference input_ref;
	
	public PipeStreamerConsumer(int chunk_size, Reference input_ref, boolean may_stream, boolean sole_consumer, boolean must_block, boolean do_log) {
		this.input_ref = input_ref;
		this.may_stream = may_stream;
		this.sole_consumer = sole_consumer;
		this.chunk_size = chunk_size;
		this.must_block = must_block;
		this.do_log = do_log;
	}
	
	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	@Override
	public void invoke() throws Exception {
		
		Ciel.blockOn(this.input_ref);
		
		InputStream in = null;
		
		if(!this.may_stream) {
			String filename = Ciel.RPC.getFilenameForReference(this.input_ref);
			in = new FileInputStream(filename);
		}
		else {
			in = Ciel.RPC.getStreamForReference(this.input_ref, this.chunk_size, this.sole_consumer, false, this.must_block);
		}
		byte[] inputBuffer = new byte[4096];
		long bytes_read = 0;
		
		while(true) {
			int this_read = in.read(inputBuffer);
			if(this_read == -1) {
				break;
			}
			else {
				bytes_read += this_read;
			}
		}
		
		Ciel.returnPlainString("Consumer read " + bytes_read + " bytes");
		
	}

	@Override
	public void setup() {
		;
	}

}
