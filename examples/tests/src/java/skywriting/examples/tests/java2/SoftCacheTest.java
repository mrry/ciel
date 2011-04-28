package skywriting.examples.tests.java2;

import java.io.OutputStream;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.references.WritableReference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class SoftCacheTest implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	@Override
	public void invoke() throws Exception {
		
		WritableReference out_file = Ciel.RPC.getNewObjectFilename("words");
		OutputStream o = out_file.open();
		for(int i = 0; i < 5000; i++) {
			o.write(i % 128);
		}
		o.close();
		Reference out_ref = out_file.getCompletedRef();
		
		FirstClassJavaTask child1 = new SoftCacheChild(out_ref);
		FirstClassJavaTask child2 = new SoftCacheChild(out_ref);
		
		Reference[] refs1 = Ciel.spawn(child1, new String[0], 1);
		Reference[] refs2 = Ciel.spawn(child2, new String[0], 1);
		
		Ciel.blockOn(refs1[0], refs2[0]);
		
		String ret = "My first child returned '" + Ciel.stringOfRef(refs1[0]) + "' and the second returned '" + Ciel.stringOfRef(refs2[0]) + "'\n";
		Ciel.returnPlainString(ret);
		
	}

	@Override
	public void setup() {
		;
	}

}
