package skywriting.examples.tests.java2;

import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class PipeStreamerTest implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	@Override
	public void invoke() throws Exception {
		
		int n_chunks = Integer.parseInt(Ciel.args[0]);
		String mode = Ciel.args[1];
		boolean do_log = Boolean.parseBoolean(Ciel.args[2]);
		
		boolean producer_pipe;
		boolean consumer_pipe;
		boolean consumer_must_block;
		boolean producer_may_stream;
		boolean consumer_may_stream;
		
	    if(mode.equals("sync")) {
	        producer_pipe = false;
	        consumer_pipe = false;
	        consumer_must_block = false;
	        producer_may_stream = false;
	        consumer_may_stream = false;	        
	    }
	    else if(mode.equals("indirect_single_fetch")) {
	        producer_pipe = false;
	        consumer_pipe = false;
	        consumer_must_block = false;
	        producer_may_stream = false;
	        consumer_may_stream = true;
	    }
	    else if(mode.equals("indirect_pipe")) {
	        producer_pipe = false;
	        consumer_pipe = false;
	        consumer_must_block = true;
	        producer_may_stream = true;
	        consumer_may_stream = true;	        
	    }
	    else if(mode.equals("indirect")) {
	        producer_pipe = false;
	        consumer_pipe = false;
	        consumer_must_block = false;
	        producer_may_stream = true;
	        consumer_may_stream = true;	        
	    }
	    else if(mode.equals("indirect_tcp")) {
	        producer_pipe = false;
	        consumer_pipe = true;
	        consumer_must_block = false;
	        producer_may_stream = true;
	        consumer_may_stream = true;	        
	    }
	    else if (mode.equals("direct")) {
	        producer_pipe = true;
	        consumer_pipe = true;
	        consumer_must_block = false;
	        producer_may_stream = true;
	        consumer_may_stream = true;	        
	    }
	    else {
	        throw new Exception("PipeStreamerTest: bad mode " + mode);
	    }
	    
		FirstClassJavaTask producer = new PipeStreamerProducer(67108864, n_chunks, producer_may_stream, producer_pipe);
		Reference[] producer_refs = Ciel.spawn(producer, new String[0], 2);
	    FirstClassJavaTask consumer = new PipeStreamerConsumer(67108864, producer_refs[1], consumer_may_stream, consumer_pipe, consumer_must_block, do_log);
	    Reference[] consumer_refs = Ciel.spawn(consumer, new String[0], 1);
		
		Ciel.blockOn(producer_refs[0], consumer_refs[0]);
		
		String ret = "My first child returned '" + Ciel.stringOfRef(producer_refs[0]) + "' and the second returned '" + Ciel.stringOfRef(consumer_refs[0]) + "'\n";
		Ciel.returnPlainString(ret);
		
	}

	@Override
	public void setup() {
		;
	}

}
