package edu.jlime.metrics.metric;

import java.util.Map;

import edu.jlime.metrics.meters.Accumulator;
import edu.jlime.metrics.meters.Counter;
import edu.jlime.metrics.meters.Gauge;
import edu.jlime.metrics.meters.Meter;
import edu.jlime.metrics.meters.Simple;

public interface IMetrics {

	public abstract Metric<?> get(String k);

	public abstract MetricList list(String k);

	public abstract Meter meter(String k);

	public abstract Simple simple(String k);

	public abstract Gauge gauge(String k);

	public abstract Accumulator accumulator(String k);

	public abstract void simple(String k, Object v);

	public abstract Counter counter(String k);

	public abstract Map<String, Metric<?>> getAll(String root);

}