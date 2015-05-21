package edu.jlime.pregel.aggregators;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.worker.DoubleAggregator;

public class DoubleDifferenceAggregator implements DoubleAggregator {

	double old = Double.MIN_VALUE;
	double newVal = 0d;
	double currentDiff;

	@Override
	public void superstep(int s) {
		if (old != Double.MIN_VALUE)
			currentDiff = old - newVal;
		old = newVal;
		newVal = 0d;
	}

	@Override
	public synchronized void add(long from, long to, double value) {
		newVal += value;
	}

	@Override
	public double get() {
		System.out.println("Returning difference " + currentDiff + "");
		return currentDiff;
	}

	@Override
	public Aggregator copy() {
		return new DoubleDifferenceAggregator();
	}

	@Override
	public void reset() {
		newVal = 0d;
	}

	@Override
	public void merge(Aggregator value) {
		newVal += ((DoubleAggregator) value).get();
	}
}