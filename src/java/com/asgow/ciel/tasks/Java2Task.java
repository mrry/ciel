package com.asgow.ciel.tasks;

import com.asgow.ciel.executor.ExecutionContext;

public interface Java2Task {

	void invoke(TaskDescriptor descriptor, ExecutionContext context);
	
}
