
package skywriting.examples.tests.testpackage;

import uk.co.mrry.mercator.task.Task;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;

public class Test2 implements Task {

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

	System.out.printf("Test1: I have %d inputs, %d outputs, and %d arguments:\n", fis.length, fos.length, args.length);
	for(int i = 0; i < args.length; i++) {
	    System.out.println(args[i]);
	}
	try {
	    System.out.printf("My inputs begin with: %c, %c\n", (char)fis[0].read(), (char)fis[1].read());
	    fos[0].write(45);
	    fos[1].write(46);
	}
	catch(IOException e) {
	    
	    System.out.println("IOException using my inputs or outputs: " + e.toString());

	}

    }

}