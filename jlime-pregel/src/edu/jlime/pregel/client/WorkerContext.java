package edu.jlime.pregel.client;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.CacheManager;
import edu.jlime.pregel.worker.WorkerTask;

public class WorkerContext {
	private WorkerTask task;

	private long v;

	private CacheManager cache;

	public WorkerContext(WorkerTask task, CacheManager cacheManager, long v) {
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
		cache.send(type, this.v, to, curr);
	}

	public void sendAll(String msgType, Object val) throws Exception {
		cache.sendAll(msgType, this.v, val);
	}

	public void sendFloat(String type, long to, float curr) throws Exception {
		cache.sendFloat(type, this.v, to, curr);

	}

	public void sendAllFloat(String msgType, float val) throws Exception {
		cache.sendAllFloat(msgType, this.v, val);

	}

	public void sendAllDouble(String msgType, double val) throws Exception {
		cache.sendAllDouble(msgType, this.v, val);

	}

	public void sendDouble(String type, long to, double val) throws Exception {
		cache.sendDouble(type, this.v, to, val);
	}

	public void setCurrVertex(long currentVertex) {
		this.v = currentVertex;
	}

	public Aggregator getAggregator(String string) {
		return task.getAggregator(string);
	}
}
