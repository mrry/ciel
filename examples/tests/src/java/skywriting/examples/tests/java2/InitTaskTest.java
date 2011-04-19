package skywriting.examples.tests.java2;

import com.asgow.ciel.executor.Ciel;
import com.asgow.ciel.tasks.SingleOutputTask;

public class InitTaskTest extends SingleOutputTask<Integer> {

	@Override
	public Integer run() {
		System.out.println("^^^^^^^^^^^^^^^^^^^^ In InitTaskTest.run()");
		System.out.println("Ciel.args[0] = " + Ciel.args[0]);
		Integer ret = Integer.parseInt(Ciel.args[0]);
		System.out.println("Return value = " + ret);
		return ret;
	}

}
