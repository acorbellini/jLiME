package edu.jlime.pregel;

import java.io.Serializable;
import java.util.HashMap;

import edu.jlime.pregel.coordinator.Aggregator;

public class PregelExecution implements Serializable {
	int steps;
	int taskID;
	private HashMap<String, Aggregator> agg;

	public PregelExecution(int steps, int taskID,
			HashMap<String, Aggregator> aggregators) {
		super();
		this.steps = steps;
		this.taskID = taskID;
		this.agg = aggregators;
	}

	public int getSteps() {
		return steps;
	}

	public int getTaskID() {
		return taskID;
	}

	public Aggregator getAgg(String agg2) {
		return agg.get(agg2);
	}
}
