package edu.jlime.pregel.client;

import edu.jlime.pregel.PregelSubgraph;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.CacheManagerI;
import edu.jlime.pregel.worker.WorkerTask;

public class WorkerContext {
	private WorkerTask task;

	private long v;

	private CacheManagerI cache;

	public WorkerContext(WorkerTask task, CacheManagerI cacheManager, long v) {
		this.task = task;
		this.v = v;
		this.cache = cacheManager;
	}

	public Graph getGraph() {
		return task.getGraph();
	};

	public Integer getSuperStep() {
		return task.getSuperStep();
	};

	public void send(String type, long to, Object curr) throws Exception {
		if (task.isLocal(to))
			task.outputObject(type, v, to, curr);
		else
			cache.send(type, this.v, to, curr);
	}

	public void sendAll(String msgType, Object val) throws Exception {
		cache.sendAll(msgType, this.v, val);
	}

	public void sendFloat(String type, long to, float curr) throws Exception {
		if (task.isLocal(to))
			task.outputFloat(type, v, to, curr);
		else
			cache.sendFloat(type, this.v, to, curr);

	}

	public void sendAllFloat(String msgType, float val) throws Exception {
		cache.sendAllFloat(msgType, this.v, val);
	}

	public void sendAllDouble(String msgType, double val) throws Exception {
		cache.sendAllDouble(msgType, this.v, val);

	}

	public void sendDouble(String type, long to, double val) throws Exception {
		if (task.isLocal(to))
			task.outputDouble(type, v, to, val);
		else
			cache.sendDouble(type, this.v, to, val);
	}

	public void setCurrVertex(long currentVertex) {
		this.v = currentVertex;
	}

	public Aggregator getAggregator(String string) {
		return task.getAggregator(string);
	}

	public PregelSubgraph getSubGraph(String string) {
		return task.getSubgraph(string);
	}

	public void sendAllSubGraph(String msgType, String subgraph, Object val)
			throws Exception {
		cache.sendAllSubGraph(msgType, subgraph, this.v, val);
	}

	public void sendAllFloatSubGraph(String msgType, String subgraph, float val)
			throws Exception {
		cache.sendAllFloatSubGraph(msgType, subgraph, this.v, val);
	}
}
