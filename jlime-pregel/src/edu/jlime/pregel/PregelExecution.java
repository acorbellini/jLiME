package edu.jlime.pregel;

import java.io.Serializable;

public class PregelExecution implements Serializable {
	int steps;
	int taskID;

	public PregelExecution(int steps, int taskID) {
		super();
		this.steps = steps;
		this.taskID = taskID;
	}

	public int getSteps() {
		return steps;
	}

	public int getTaskID() {
		return taskID;
	}
}
