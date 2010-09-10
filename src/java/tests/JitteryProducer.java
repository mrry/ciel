
import uk.co.mrry.mercator.task.Task;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Random;
import java.util.List;
import java.util.LinkedList;

class StreamFeeder {

    private FileOutputStream fos;
    private boolean active;
    private int last_number_written;
    private Random rng;
    private int id;

    public StreamFeeder(FileOutputStream fos, Random rng, int id) {

	this.fos = fos;
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

    public void invoke(FileInputStream[] fis, FileOutputStream[] fos, String[] args) {

	Random rng = new Random();
	List<StreamFeeder> streams = new LinkedList<StreamFeeder>();

	for (int i = 0; i < fos.length; i++) {

	    streams.add(new StreamFeeder(fos[i], rng, i));

	}

	try {
	    long seconds_elapsed = 0;
	    long last_number_written = 0;
	    System.out.printf("Producer start\n");
	    while(seconds_elapsed < 10) {
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