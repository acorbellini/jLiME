package edu.jlime.metrics.sysinfo.filter;

import java.io.Serializable;
import java.util.HashMap;

import edu.jlime.metrics.metric.CompositeMetrics;

public interface SysInfoFilter<T> extends Serializable {

	public HashMap<T, Float> extract(CompositeMetrics<T> info);
}
