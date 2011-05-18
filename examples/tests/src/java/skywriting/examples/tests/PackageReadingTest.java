package skywriting.examples.tests;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class PackageReadingTest implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void invoke() throws Exception {
		Reference[] refs = Ciel.getRefsFromPackage("pack_input");
		
		for (int i = 0; i < refs.length; ++i) {
			System.out.println(refs[i]);
			System.out.println(Ciel.stringOfRef(refs[i]).charAt(i));
		}
		
		Ciel.returnPlainString("done, read " + refs.length + " refs");
	}

	@Override
	public void setup() {
		
	}

}
