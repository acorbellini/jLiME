package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metrics;

public class Accumulator extends Meter implements AccumulatorMBean {

	/**
	 * 
	 */
	private static final long serialVersionUID = 511897837131539698L;
	Float accumulated = 0f;

	@Override
	public void update(Float val) {
		super.update(accumulated - val);
		accumulated = val;
	}

	@Override
	public String toString() {
		return Metrics.format("acc=" + accumulated, super.toString());
	}

	@Override
	public Float getValue() {
		return accumulated;
	}

}
