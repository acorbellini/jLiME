package edu.jlime.metrics.meters;

import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;

public class Meter implements Metric<Float>, MeterMBean {

	/**
	 * 
	 */
	private static final long serialVersionUID = -7321038067082719726L;

	Gauge g = new Gauge();

	float meterValue = 0f;

	Long lastTime = -1l;

	float perf = 0f;

	// private float TIME = 5;

	private float alpha = 0.8f;

	@Override
	public void update(Float val) {
		long currentTime = System.nanoTime();
		if (lastTime == -1l) {
			lastTime = currentTime;
			meterValue = val;
			return;
		}

		// http://en.wikipedia.org/wiki/Moving_average
		float timeDiff = (currentTime - lastTime) / 1000000000f;
		// double alpha = 1 - Math.exp(-timeDiff / TIME);

		lastTime = currentTime;

		float avg = val / timeDiff;

		perf = alpha * avg + (1 - alpha) * perf;

		meterValue = val;

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

	@Override
	public String get() {
		return meterValue + "";
	}

	@Override
	public Metric<Float> copy() {
		Meter ret = new Meter();
		ret.g = (Gauge) this.g.copy();
		ret.lastTime = this.lastTime;
		ret.meterValue = this.meterValue;
		ret.perf = this.perf;
		return ret;
	}
}
