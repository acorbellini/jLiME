package edu.jlime.metrics.sysinfo.filter;

import java.util.HashMap;

import edu.jlime.metrics.metric.CompositeMetrics;
import edu.jlime.metrics.metric.Metric;
import edu.jlime.metrics.metric.Metrics;

public class SimpleInfoFilter<T> implements SysInfoFilter<T> {

	private static final long serialVersionUID = 8441347633843816100L;

	private String sel;

	public SimpleInfoFilter(String sel) {
		this.sel = sel;
	}

	@Override
	public HashMap<T, Float> extract(CompositeMetrics<T> info) {
		HashMap<T, Float> vals = new HashMap<>();
		for (T p : info.getKeys()) {
			vals.put(p, get(info.get(p)));
		}
		return vals;
	}

	public Float get(Metrics info) {
		Metric<?> metric = info.get(sel);
		return Float.valueOf(metric.get());
	}

	@Override
	public String toString() {
		return sel;
	}

}
