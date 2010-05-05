import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import uk.co.mrry.mercator.task.Task;

public class SWTeraPreBucketer implements Task {
	
    public void invoke(InputStream[] inputs, OutputStream[] outputs, String[] args) {

	InputStream[] bucketerInputs = new InputStream[2];
	bucketerInputs[0] = null;
	bucketerInputs[1] = inputs[0];

	String[] bucketerArgs = new String[1];
	bucketerArgs[0] = ((Integer)1).toString();

	return (new SWTeraBucketer()).invoke(bucketerInputs, outputs, bucketerArgs);

    }

}