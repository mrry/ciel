package skywriting.examples.tests;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import uk.co.mrry.mercator.task.Task;

public class Test1 implements Task {

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

	System.out.printf("Test1: I have %d inputs, %d outputs, and %d arguments:\n", fis.length, fos.length, args.length);
	for(int i = 0; i < args.length; i++) {
	    System.out.println(args[i]);
	}
	try {
		try {
			while (true) {
				fis[0].read();
			}
		} catch (EOFException eofe) {
			;
		}
	    fos[0].write(45);
	    fos[0].write(46);
	}
	catch(IOException e) {
	    
	    System.out.println("IOException using my inputs or outputs: " + e.toString());

	}

    }

}