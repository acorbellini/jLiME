package edu.jlime.pregel;

import java.io.Serializable;
import java.util.UUID;

public class PregelExecution implements Serializable {
	int steps;
	UUID taskID;

	public PregelExecution(int steps, UUID taskID) {
		super();
		this.steps = steps;
		this.taskID = taskID;
	}

	public int getSteps() {
		return steps;
	}

	public UUID getTaskID() {
		return taskID;
	}
}
