package skywriting.examples.tests;


import uk.co.mrry.mercator.task.Task;
import java.io.InputStream;
import java.io.OutputStream;

public class Test3Master implements Task {

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args) {

	System.out.println("Calling slave...");
	Test3Slave s = new Test3Slave();
	s.f();
	System.out.println("Call complete");

    }

}