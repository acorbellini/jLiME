package edu.jlime.jd.profiler;

import edu.jlime.metrics.metric.Metrics;

public abstract class MetricExtractor<T> {

	public abstract T get(Metrics m);
}
