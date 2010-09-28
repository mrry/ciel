package tests;


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

    public StreamFeeder(OutputStream fos2, Random rng, int id) {

	this.fos = fos2;
	this.active = true;
	this.last_number_written = 0;
	this.rng = rng;
	this.id = id;

    }

    public void writeSome() throws Exception {

	if(this.active) {
	    for(int i = 0; i < 10000; i++) {
		String nextWrite = String.format("%d,", this.last_number_written++);
		this.fos.write(nextWrite.getBytes("US-ASCII"));
	    }	
	}

    }

    public void flipCoin() {

	this.active = this.rng.nextBoolean();
	System.out.printf("Stream %d active: %b\n", this.id, this.active);

    }

}

public class JitteryProducer implements Task {

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

	Random rng = new Random();
	List<StreamFeeder> streams = new LinkedList<StreamFeeder>();

	int seconds_to_run = Integer.parseInt(args[0]);

	for (int i = 0; i < fos.length; i++) {

	    streams.add(new StreamFeeder(fos[i], rng, i));

	}

	try {
	    long seconds_elapsed = 0;
	    long last_number_written = 0;
	    System.out.printf("Producer start\n");
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
	    System.out.printf("Producer stop\n");
	}
	catch(Exception e) {
	    System.out.printf("JitteryProducer epic fail: %s\n", e.toString());
	    throw new Error(e.toString());
	}

    }

}