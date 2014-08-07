package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;

public class Meter implements Metric<Float>, MeterMBean {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7321038067082719726L;

	Gauge g = new Gauge();

	Float meterValue;

	Long lastTime = -1l;

	Double perf;

	private double TIME = 5;

	@Override
	public void update(Float val) {
		long currentTime = System.currentTimeMillis();
		if (lastTime == -1) {
			lastTime = currentTime;
			meterValue = val;
			return;
		}
		// http://en.wikipedia.org/wiki/Moving_average
		long timeDiff = (currentTime - lastTime) / 1000;
		double alpha = 1 - Math.exp(-timeDiff / TIME);
		perf = alpha * val + (1 - alpha) * meterValue;

		g.update(val);
	}

	@Override
	public String toString() {
		return Metrics.format(g.toString(), "perf=" + perf + "/sec");
	}

	@Override
	public Float getValue() {
		return meterValue;
	}
}
