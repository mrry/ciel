package uk.co.mrry.mercator.task;

import java.util.List;

/**
 * A task takes zero or more named data items, performs some computation on them, and yields one or more concrete data items
 * as output.
 * 
 * To execute a task, the named input data must be available locally.
 * 
 * @author dgm36
 *
 */
public interface Task {

	List<String> getInputDataNames();
	List<ConcreteData> getOutputs();
	
	List<WorkerFacility> getNeededFacilities();
	
}
