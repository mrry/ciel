package skywriting.examples.tests;


import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import uk.co.mrry.mercator.task.Task;

class StreamFeeder {

    private OutputStream fos;
    private boolean active;
    private int last_number_written;
    private Random rng;
    private int id;
    public int bytes_written;

    public StreamFeeder(OutputStream fos2, Random rng, int id) {

	this.fos = fos2;
	this.active = true;
	this.last_number_written = 0;
	this.rng = rng;
	this.id = id;
	this.bytes_written = 0;

    }

    public void writeSome() throws Exception {

	if(this.active) {
	    for(int i = 0; i < 10000; i++) {
		String nextWrite = String.format("%d,", this.last_number_written++);
		byte[] nextWriteBytes = nextWrite.getBytes("US-ASCII");
		this.fos.write(nextWriteBytes);
		this.bytes_written += nextWriteBytes.length;
	    }	
	}

    }

    public void flipCoin() {

	this.active = this.rng.nextBoolean();
	System.err.printf("Stream %d active: %b\n", this.id, this.active);

    }

}

public class JitteryProducer implements Task {

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

	Random rng = new Random();
	List<StreamFeeder> streams = new LinkedList<StreamFeeder>();

	int seconds_to_run = Integer.parseInt(args[0]);

	System.err.printf("JitteryProducer: feeding %d streams for %d seconds\n", fos.length - 1, seconds_to_run);

	for (int i = 0; i < (fos.length - 1); i++) {

	    streams.add(new StreamFeeder(fos[i], rng, i));

	}

	try {
	    long seconds_elapsed = 0;
	    long last_number_written = 0;
	    System.err.printf("Producer start\n");
	    while(seconds_elapsed < seconds_to_run) {
		long last_change_time = System.currentTimeMillis();
		while(System.currentTimeMillis() < (last_change_time + 1000)) {
		    for(StreamFeeder stream : streams) {
			stream.writeSome();
		    }
		}
		seconds_elapsed++;
		for(StreamFeeder stream : streams) {

		    stream.flipCoin();

		}
	    }
	    System.err.printf("Producer stop\n");
	    int total_bytes_written = 0;
	    for(StreamFeeder stream : streams) {
		total_bytes_written += stream.bytes_written;
	    }
	    fos[fos.length - 1].write(Integer.toString(total_bytes_written).getBytes("US-ASCII"));
	}
	catch(Exception e) {
	    System.err.printf("JitteryProducer epic fail: %s\n", e.toString());
	    throw new Error(e.toString());
	}

    }

}