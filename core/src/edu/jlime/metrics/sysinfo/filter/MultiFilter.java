package edu.jlime.metrics.sysinfo.filter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.jlime.metrics.metric.CompositeMetrics;

public abstract class MultiFilter<T> implements SysInfoFilter<T> {

	private static final long serialVersionUID = -12434674720532040L;

	SysInfoFilter<T>[] filters;

	public MultiFilter(SysInfoFilter<T>[] f) {
		this.filters = f;
	}

	@Override
	public HashMap<T, Float> extract(CompositeMetrics<T> info) {
		List<HashMap<T, Float>> toMerge = new ArrayList<>();
		for (SysInfoFilter<T> fil : filters) {
			toMerge.add(fil.extract(info));
		}
		return merge(toMerge);
	}

	public abstract HashMap<T, Float> merge(List<HashMap<T, Float>> toMerge);

}
