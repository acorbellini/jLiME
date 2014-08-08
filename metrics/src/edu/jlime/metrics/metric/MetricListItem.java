package edu.jlime.metrics.metric;

import java.util.List;
import java.util.Map;

import edu.jlime.metrics.meters.Accumulator;
import edu.jlime.metrics.meters.Counter;
import edu.jlime.metrics.meters.Gauge;
import edu.jlime.metrics.meters.Meter;
import edu.jlime.metrics.meters.Simple;

public class MetricListItem implements IMetrics {

	String root;

	IMetrics metrics;

	public MetricListItem(String k, IMetrics m) {
		this.root = k;
		this.metrics = m;
	}

	public Metric<?> get(String sub) {
		return metrics.get(root + "." + sub);
	}

	public MetricList list(String sub) {
		return metrics.list(root + "." + sub);
	}

	@Override
	public Meter meter(String k) {
		return metrics.meter(root + "." + k);
	}

	@Override
	public Simple simple(String k) {
		return metrics.simple(root + "." + k);
	}

	@Override
	public Gauge gauge(String k) {
		return metrics.gauge(root + "." + k);
	}

	@Override
	public Accumulator accumulator(String k) {
		return metrics.accumulator(root + "." + k);
	}

	@Override
	public void simple(String k, Object v) {
		metrics.simple(root + "." + k, v);
	}

	@Override
	public Counter counter(String k) {
		return metrics.counter(root + "." + k);
	}

	@Override
	public String toString() {
		return metrics.getAll(root).toString();
	}

	@Override
	public Map<String, Metric<?>> getAll(String root) {
		return metrics.getAll(this.root + "." + root);
	}
}