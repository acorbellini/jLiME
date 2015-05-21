package edu.jlime.pregel.client;

import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.WorkerTask;

public class WorkerContext {
	private WorkerTask task;

	private long v;

	public WorkerContext(WorkerTask task, long v) {
		this.task = task;
		this.v = v;
	}

	public Graph getGraph() {
		return task.getGraph();
	};

	public Integer getSuperStep() {
		return task.getSuperStep();
	};

	public void send(String type, long to, Object curr) throws Exception {
		task.send(type, this.v, to, curr);

	}

	public void sendAll(String msgType, Object val) throws Exception {
		task.sendAll(msgType, this.v, val);
	}

	public void sendFloat(String type, long to, float curr) throws Exception {
		task.sendFloat(type, this.v, to, curr);

	}

	public void sendAllFloat(String msgType, float val) throws Exception {
		task.sendAllFloat(msgType, this.v, val);

	}

	public void sendAllDouble(String msgType, double val) throws Exception {
		task.sendAllDouble(msgType, this.v, val);

	}

	public void sendDouble(String type, long to, double val) throws Exception {
		task.sendDouble(type, this.v, to, val);
	}

	public void setCurrVertex(long currentVertex) {
		this.v = currentVertex;
	}

	public Aggregator getAggregator(String string) {
		return task.getAggregator(string);
	}
}
