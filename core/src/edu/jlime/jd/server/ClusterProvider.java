package edu.jlime.jd.server;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.Node;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.metric.SensorMeasure;
import edu.jlime.metrics.sysinfo.InfoProvider;

public class ClusterProvider extends InfoProvider {

	private ClientCluster c;

	public ClusterProvider(Dispatcher jd) {
		this.c = jd.getCluster();
	}

	@Override
	public void load(Metrics mgr) throws Exception {
		mgr.createTimedSensor(new SensorMeasure() {
			List<Node> before = null;

			@Override
			public void proc(Metrics mgr) throws Exception {
				ArrayList<Node> executors = c.getExecutors();
				for (Node jobNode : executors) {
					mgr.set("executors").update(jobNode.toString());
				}
				if (before != null) {
					before.removeAll(executors);
					for (Node j : before) {
						mgr.set("executors").remove(j);
					}
				}
				before = executors;
			}
		});
	}
}
