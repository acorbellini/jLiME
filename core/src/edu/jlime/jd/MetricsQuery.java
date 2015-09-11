package edu.jlime.jd;

import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import edu.jlime.metrics.metric.Metrics;

public class MetricsQuery implements Job<Metrics> {

	private static final long serialVersionUID = 1L;

	public MetricsQuery() {
	}

	@Override
	public Metrics call(JobContext env, Node peer) throws Exception {
		Metrics metrics = env.getCluster().getMetrics();
		return metrics;
	}
}
