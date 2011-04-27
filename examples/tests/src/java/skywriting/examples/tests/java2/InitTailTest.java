package skywriting.examples.tests.java2;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class InitTailTest implements FirstClassJavaTask {

	private final Reference[] dependencies;
	
	public InitTailTest(Reference[] dependencies) {
		this.dependencies = dependencies;
	}
	
	@Override
	public Reference[] getDependencies() {
		return this.dependencies;
	}

	@Override
	public void invoke() throws Exception {
		System.out.println("££££££££££££££££££££££££££££££ InitTailTest");
		for (Reference ref : dependencies) {
			String filename = Ciel.RPC.getFilenameForReference(ref);
			System.out.println(filename);
		}
		
		Ciel.returnPlainString("Hello world!");
		
		Ciel.log("Finished InitTailTest!");
	}

	@Override
	public void setup() {
		;
	}

}
