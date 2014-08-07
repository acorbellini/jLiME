package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;

public class Counter extends Accumulator implements Metric<Float> {

	public void count() {
		super.update(super.accumulated + 1f);
	}
}
