package edu.jlime.graphly.util;

import com.google.common.util.concurrent.AtomicDouble;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.worker.FloatAggregator;

public class FloatSumAggregator implements FloatAggregator {

	AtomicDouble curr = new AtomicDouble(0d);
	AtomicDouble old = new AtomicDouble(0d);

	@Override
	public void superstep(int s) {
		reset();
	}

	@Override
	public Aggregator copy() {
		FloatSumAggregator floatSumAggregator = new FloatSumAggregator();
		floatSumAggregator.curr = new AtomicDouble(curr.get());
		floatSumAggregator.old = new AtomicDouble(old.get());
		return floatSumAggregator;
	}

	@Override
	public synchronized void reset() {
		AtomicDouble aux = old;
		old = curr;
		curr = aux;
		curr.set(0d);
	}

	@Override
	public void merge(Aggregator value) {
		old.addAndGet(((FloatAggregator) value).get());
		curr.addAndGet(((FloatAggregator) value).getCurrent());
	}

	@Override
	public void add(long from, long to, float val) {
		curr.addAndGet(val);
	}

	@Override
	public float get() {
		return (float) old.get();
	}

	@Override
	public float getCurrent() {
		return (float) curr.get();
	}

}
