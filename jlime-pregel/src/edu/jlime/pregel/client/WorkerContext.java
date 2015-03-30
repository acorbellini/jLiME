package edu.jlime.pregel.client;

import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.PregelMessage;
import edu.jlime.pregel.worker.VertexData;
import edu.jlime.pregel.worker.WorkerTask;

public class WorkerContext {
	private WorkerTask task;

	private Long v;

	public WorkerContext(WorkerTask task, Long v) {
		this.task = task;
		this.v = v;
	}

	public void send(Long to, VertexData data) throws Exception {
	};

	public Graph getGraph() {
		return task.getGraph();
	};

	public void setHalted() {
		task.setHalted(v);
	};

	public Integer getSuperStep() {
		return task.getSuperStep();
	};

	public Double getAggregatedValue(String string) throws Exception {
		return task.getAggregatedValue(v, string);
	}

	public void setAggregatedValue(String string, double currentVal)
			throws Exception {
		task.setAggregatedValue(v, string, currentVal);
	}

	public void send(long to, Object val) throws Exception {
		task.send(new PregelMessage(this.v, to, val));
	}

	public void sendAll(Object val) throws Exception {
		task.sendAll(new PregelMessage(this.v, -1, val));
	};
}
