package edu.jlime.metrics.sysinfo;

import edu.jlime.metrics.metric.Metrics;

public abstract class InfoProvider {

	public abstract void load(Metrics mgr) throws Exception;
}
