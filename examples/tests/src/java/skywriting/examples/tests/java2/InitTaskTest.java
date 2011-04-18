package skywriting.examples.tests.java2;

import com.asgow.ciel.tasks.SingleOutputTask;

public class InitTaskTest extends SingleOutputTask<Integer> {

	@Override
	public Integer run() {
		System.out.println("In InitTaskTest.run()");
		return 42;
	}

}
