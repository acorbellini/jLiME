package edu.jlime.pregel.client;

import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.FloatPregelMessage;
import edu.jlime.pregel.worker.GenericPregelMessage;
import edu.jlime.pregel.worker.WorkerTask;

public class WorkerContext {
	private WorkerTask task;

	private long v;

	// private ArrayList<PregelMessage> toSend = new ArrayList<>();

	public WorkerContext(WorkerTask task, long v) {
		this.task = task;
		this.v = v;
	}

	public Graph getGraph() {
		return task.getGraph();
	};

	// public void setHalted() {
	// task.setHalted(v);
	// };

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

	public void send(long to, Object curr) throws Exception {
		task.send(this.v, to, curr);

	}

	public void sendAll(Object val) throws Exception {
		task.sendAll(this.v, val);
	}

	public void sendFloat(long to, float curr) throws Exception {
		task.sendFloat(this.v, to, curr);

	}

	public void sendAllFloat(float val) throws Exception {
		task.sendAllFloat(this.v, -1, val);

	}
}
