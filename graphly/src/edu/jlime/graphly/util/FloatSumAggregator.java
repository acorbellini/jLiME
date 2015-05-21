package edu.jlime.graphly.util;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.worker.FloatAggregator;

public class FloatSumAggregator implements FloatAggregator {

	float sum = 0f;

	@Override
	public void superstep(int s) {
		reset();
	}

	@Override
	public Aggregator copy() {
		return new FloatSumAggregator();
	}

	@Override
	public void reset() {
		sum = 0f;
	}

	@Override
	public void merge(Aggregator value) {
		sum += ((FloatAggregator) value).get();
	}

	@Override
	public void add(long from, long to, float val) {
		sum += val;
	}

	@Override
	public float get() {
		return sum;
	}

}
