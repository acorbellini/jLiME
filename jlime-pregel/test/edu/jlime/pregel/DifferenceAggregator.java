package edu.jlime.pregel;

import java.util.HashMap;
import java.util.Map.Entry;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.Vertex;

final class DifferenceAggregator implements Aggregator {
	HashMap<Vertex, Double> values = new HashMap<>();
	Double old;
	Double currentDiff;

	@Override
	public void superstep(int s) {
		if (old != null)
			currentDiff = old - getAcc();
		old = getAcc();
	}

	private Double getAcc() {
		double acc = 0d;
		for (Entry<Vertex, Double> e : values.entrySet()) {
			acc += e.getValue();
		}
		return acc;
	}

	@Override
	public synchronized void setVal(Vertex v, Double value) {
		values.put(v, value);
	}

	@Override
	public Double getVal(Vertex v) {
		System.out.println("Returning difference for vertex " + v + " ("
				+ currentDiff + ")");
		return currentDiff;
	}
}