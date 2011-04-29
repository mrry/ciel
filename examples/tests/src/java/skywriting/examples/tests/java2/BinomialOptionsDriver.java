package skywriting.examples.tests.java2;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class BinomialOptionsDriver implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}
	
	@Override
	public void invoke() throws Exception {
		
		int n = Integer.parseInt(Ciel.args[0]);
		int chunk_size = Integer.parseInt(Ciel.args[1]);
		
		double s = 100;
		double k = 100;
		double t = 1;
		double v = 0.3;
		double rf = 0.03;
		double cp = -1;
		
		FirstClassJavaTask lastNode = new BinomialOptions(null, s, k, t, v, rf, cp, n, chunk_size, true);
	    Reference[] last_refs = Ciel.spawn(lastNode, new String[0], 1);
		for(int i = 0; i < n; i+= chunk_size) {
			lastNode = new BinomialOptions(last_refs[0], s, k, t, v, rf, cp, n, chunk_size, false);
			last_refs = Ciel.spawn(lastNode, new String[0], 1);
		}
		
		Ciel.blockOn(last_refs[0]);
		
		Ciel.returnPlainString("The last node returned: " + Ciel.stringOfRef(last_refs[0]) + "\n");
		
	}

	@Override
	public void setup() {
		;
	}

}
