package edu.jlime.pregel.worker;

import edu.jlime.pregel.coordinator.Aggregator;

public interface DoubleAggregator extends Aggregator {
	public void add(long from, long to, double val);

	public double getDouble();
}
