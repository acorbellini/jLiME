package edu.jlime.metrics.sysinfo.filter;

import java.util.HashMap;

import edu.jlime.metrics.metric.CompositeMetrics;

public abstract class WeightedFilter<T> implements SysInfoFilter<T> {

	private static final long serialVersionUID = -9083848847145127925L;

	private SysInfoFilter<T> filter;

	public WeightedFilter(SysInfoFilter<T> f) {
		this.filter = f;
	}

	public HashMap<T, Float> extract(CompositeMetrics<T> info) {
		HashMap<T, Float> vals = filter.extract(info);
		return weight(vals);
	}

	public abstract HashMap<T, Float> weight(HashMap<T, Float> vals);
}
