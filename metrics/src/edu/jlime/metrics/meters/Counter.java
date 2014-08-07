package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;

public class Counter extends Accumulator implements Metric<Float> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3082841636977028358L;

	public void count() {
		super.update(super.accumulated + 1f);
	}
}
