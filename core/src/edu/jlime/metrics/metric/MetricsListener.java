package edu.jlime.metrics.metric;

public interface MetricsListener {

	public void metricAdded(String k, Metric<?> m);

	public void metricDeleted(String k, Metric<?> m);
}
