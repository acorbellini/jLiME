package edu.jlime.pregel.aggregators;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.worker.FloatAggregator;

public class FloatDifferenceAggregator implements FloatAggregator {

	float old = Float.MIN_VALUE;
	float newVal = 0f;
	float currentDiff = Float.MAX_VALUE;

	@Override
	public void superstep(int s) {
		if (old != Float.MIN_VALUE)
			currentDiff = old - newVal;
		old = newVal;
		newVal = 0f;
	}

	@Override
	public synchronized void add(long from, long to, float value) {
		newVal += value;
	}

	@Override
	public float get() {
		return newVal;
	}

	public float getDiff() {
		return this.currentDiff;
	}

	@Override
	public Aggregator copy() {
		return new FloatDifferenceAggregator();
	}

	@Override
	public void reset() {
		newVal = 0f;
	}

	@Override
	public void merge(Aggregator value) {
		newVal += ((FloatAggregator) value).get();
	}
}