package edu.jlime.pregel.worker;

import edu.jlime.pregel.coordinator.Aggregator;

public interface GenericAggregator extends Aggregator {
	public void add(long from, long to, Object val);

	public Object get();
}
