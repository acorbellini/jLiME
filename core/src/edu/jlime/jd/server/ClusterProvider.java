package edu.jlime.jd.server;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.InfoProvider;

public class ClusterProvider extends InfoProvider {

	private ClientCluster c;

	public ClusterProvider(JobDispatcher jd) {
		this.c = jd.getCluster();
	}

	@Override
	public void load(Metrics mgr) throws Exception {
		mgr.createTimedSensor(new SensorMeasure() {
			List<ClientNode> before = null;

			@Override
			public void proc(Metrics mgr) throws Exception {
				ArrayList<ClientNode> executors = c.getExecutors();
				for (ClientNode jobNode : executors) {
					mgr.set("executors").update(jobNode.toString());
				}
				if (before != null) {
					before.removeAll(executors);
					for (ClientNode j : before) {
						mgr.set("executors").remove(j);
					}
				}
				before = executors;
			}
		});
	}
}
