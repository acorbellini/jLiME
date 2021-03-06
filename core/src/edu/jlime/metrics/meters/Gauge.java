package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;

public class Gauge implements Metric<Float>, GaugeMBean {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6115039270311056716L;

	Float min = Float.MAX_VALUE;

	Float max = Float.MIN_VALUE;

	Float current = 0f;

	Float prom = 0f;

	int counts = 0;

	long before = -1;

	@Override
	public void update(Float val) {
		if (val > max)
			max = val;
		if (val < min)
			min = val;
		if (current != val)
			current = val;

		prom = ((prom * counts) + val) / (counts + 1);
		counts++;
	}

	@Override
	public String toString() {
		return Metrics.format("val=" + current, "min=" + min, "max=" + max, "prom=" + prom);
	}

	@Override
	public Float getValue() {
		return current;
	}

	@Override
	public String get() {
		return current.toString();
	}

	@Override
	public Metric<Float> copy() {
		Gauge g = new Gauge();
		g.before = this.before;
		g.max = this.max;
		g.min = this.min;
		g.current = this.current;
		g.prom = this.prom;
		g.counts = this.counts;
		return g;
	}

}
