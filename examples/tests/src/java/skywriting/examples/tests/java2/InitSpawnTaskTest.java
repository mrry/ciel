package skywriting.examples.tests.java2;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.references.Reference;
import com.asgow.ciel.tasks.FirstClassJavaTask;

public class InitSpawnTaskTest implements FirstClassJavaTask {

	@Override
	public Reference[] getDependencies() {
		return new Reference[0];
	}

	@Override
	public void invoke() throws Exception {
		
		Ciel.log("In InitSpawnTaskTest!");
		
		Reference r1 = Ciel.spawn(InitTaskTest.class, new String[] { "42" });
		Reference r2 = Ciel.spawn(InitTaskTest.class, new String[] { "16" });
		Reference r3 = Ciel.spawn(InitTaskTest.class, new String[] { "10" });
		
		FirstClassJavaTask tail = new InitTailTest(new Reference[] { r1, r2, r3 });
		
		Ciel.tailSpawn(tail, new String[0]);
		
	}

	@Override
	public void setup() {
		;
	}

}
