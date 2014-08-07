package edu.jlime.server;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.JobNode;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.InfoProvider;

public class ClusterProvider extends InfoProvider {

	private JobCluster c;

	public ClusterProvider(JobDispatcher jd) {
		this.c = jd.getCluster();
	}

	@Override
	public void load(Metrics mgr) throws Exception {
		mgr.createTimedSensor(new SensorMeasure() {
			List<JobNode> before = null;

			@Override
			public void proc(Metrics mgr) throws Exception {
				ArrayList<JobNode> executors = c.getExecutors();
				for (JobNode jobNode : executors) {
					mgr.set("executors").update(jobNode.toString());
				}
				if (before != null) {
					before.removeAll(executors);
					for (JobNode j : before) {
						mgr.set("executors").remove(j);
					}
				}
				before = executors;
			}
		});
	}
}
