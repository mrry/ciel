package skywriting.examples.tests.java2;

import java.io.FileInputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class FixedTaskTest implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		Reference r1 = Ciel.spawn(InitTaskTest.class, new String[] { "42" });
		Reference r2 = Ciel.spawn(InitTaskTest.class, new String[] { "16" });
		Reference r3 = Ciel.spawn(InitTaskTest.class, new String[] { "10" });
		
		Reference[] dependencies = {r1, r2, r3};
		
		Ciel.blockOn(r1, r2, r3);
		
		String[] outputs = new String[3];
		
		int i = 0;
		for (Reference ref : dependencies) {
			String filename = Ciel.RPC.getFilenameForReference(ref);
			FileInputStream fis = new FileInputStream(filename);
			byte[] buffer = new byte[1024];
			int read = fis.read(buffer);
			outputs[i++] = new String(buffer, 0, read);
		}

		String retval = String.format("My children returned '%s', '%s' and '%s'", outputs[0], outputs[1], outputs[2]);
		Ciel.returnPlainString(retval);
		
	}

	@Override
	public void setup() {
		;
	}

}
