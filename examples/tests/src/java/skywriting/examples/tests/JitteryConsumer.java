package skywriting.examples.tests;


import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import uk.co.mrry.mercator.task.Task;

class StreamConsumer extends Thread {

    private InputStream fis;
    private boolean active;
    private Random rng;
    private int id;
    private byte[] buffer;
    
    public int bytes_read;
    public boolean is_finished;

    public StreamConsumer(InputStream fis2, int id) {

	this.fis = fis2;
	this.active = true;
	this.rng = new Random();
	this.id = id;
	this.buffer = new byte[1024];
	this.is_finished = false;
	this.bytes_read = 0;

    }

    public void run() {

	try {
	    System.out.printf("Consumer %d start\n", this.id);
	    while(!this.is_finished) {
		long last_change_time = System.currentTimeMillis();
		while((System.currentTimeMillis() < (last_change_time + 1000)) && !this.is_finished) {
		    readSome();
		}
		flipCoin();
	    }
	    System.out.printf("Consumer %d done\n", this.id);
	}
	catch(Exception e) {
	    System.out.printf("StreamConsumer epic fail: %s\n", e.toString());
	    throw new Error(e.toString());
	}
	

    }

    public void readSome() throws Exception {

	int ret = this.fis.read(buffer, 0, 1024);
	if(ret == -1) {
	    System.out.printf("Stream %d finished\n", this.id);
	    this.is_finished = true;
	}
	else {
	    this.bytes_read += ret;
	}

    }

    public void flipCoin() throws Exception {

	if(this.rng.nextBoolean()) {
	    System.out.printf("Stream %d consumer sleeps\n", this.id);
	    Thread.sleep(1000);
	}


    }

}

public class JitteryConsumer implements Task {

    private List<StreamConsumer> streams;

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

	try {

	    streams = new LinkedList<StreamConsumer>();
	
	    for (int i = 0; i < fis.length; i++) {
		
		StreamConsumer this_consumer = new StreamConsumer(fis[i], i);
		this_consumer.start();
		streams.add(this_consumer);
		
	    }
	    
	    for (StreamConsumer stream : streams) {
		
		stream.join();
		
	    }
	    
	    System.out.printf("Consumer: all threads in, dying\n");
	    int total_bytes_read = 0;
	    for(StreamConsumer stream : streams) {
		total_bytes_read += stream.bytes_read;
	    }
	    fos[0].write(Integer.toString(total_bytes_read).getBytes("US-ASCII"));

	}
	
	catch(Exception e) {

	    System.out.printf("JitteryConsumer epic fail: %s\n", e.toString());
	    throw new Error(e.toString());

	}

    }

}