package edu.jlime.metrics.meters;

import java.util.HashSet;
import java.util.Set;

import edu.jlime.metrics.metric.Metric;

public class MetricSet implements Metric<Object>, MetricSetMBean {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3173481557892978052L;
	Set<Object> set = new HashSet<Object>();

	@Override
	public void update(Object val) {
		set.add(val);
	}

	@Override
	public String toString() {
		return set.toString();
	}

	@Override
	public Set<Object> getSet() {
		return set;
	}

	public void remove(Object string) {
		set.remove(string);
	}

	@Override
	public String get() {
		return set.toString();
	}

}
