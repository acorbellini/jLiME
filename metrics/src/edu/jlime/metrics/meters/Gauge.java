package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;

public class Gauge implements Metric<Float>, GaugeMBean {

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
		return Metrics.format("val=" + current, "min=" + min, "max=" + max,
				"prom=" + prom);
	}

	@Override
	public Float getValue() {
		return current;
	}

}
