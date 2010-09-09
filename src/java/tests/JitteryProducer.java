
import uk.co.mrry.mercator.task.Task;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Random;

public class JitteryProducer implements Task {

    public void invoke(FileInputStream[] fis, FileOutputStream[] fos, String[] args) {

	Random rng = new Random();

	try {
	    long seconds_elapsed = 0;
	    long last_number_written = 0;
	    System.out.printf("Producer start\n");
	    while(seconds_elapsed < 10) {
		if(rng.nextBoolean()) {
		    long last_change_time = System.currentTimeMillis();
		    while(System.currentTimeMillis() < (last_change_time + 1000)) {
			for(int i = 0; i < 10000; i++) {
			    String nextWrite = String.format("%d,", last_number_written++);
			    fos[0].write(nextWrite.getBytes("US-ASCII"));
			}
		    }
		}
		else {
		    Thread.sleep(1000);
		}
		seconds_elapsed++;
	    }
	    System.out.printf("Producer stop\n");
	}
	catch(Exception e) {
	    System.out.printf("JitteryProducer epic fail: %s\n", e.toString());
	    throw new Error(e.toString());
	}

    }

}