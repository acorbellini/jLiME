package edu.jlime.pregel.aggregators;

import edu.jlime.pregel.coordinator.Aggregator;

final class DifferenceAggregator implements Aggregator {
	// HashMap<Long, Double> values = new HashMap<>();
	Double old;
	Double newVal = 0d;
	Double currentDiff;

	@Override
	public void superstep(int s) {
		if (old != null)
			currentDiff = old - newVal;
		old = newVal;
	}

	// private Double getAcc() {
	// double acc = 0d;
	// for (Entry<Long, Double> e : values.entrySet()) {
	// acc += e.getValue();
	// }
	// return acc;
	// }

	@Override
	public synchronized void setVal(long v, Double value) {
		// values.put(v, value);
		newVal += value;
	}

	@Override
	public Double getVal(long v) {
		System.out.println("Returning difference " + currentDiff + "");
		return currentDiff;
	}
}