package edu.jlime.pregel.worker;

import edu.jlime.pregel.coordinator.Aggregator;

public interface FloatAggregator extends Aggregator {
	public void add(long from, long to, float val);

	public float get();

	public float getCurrent();
}
