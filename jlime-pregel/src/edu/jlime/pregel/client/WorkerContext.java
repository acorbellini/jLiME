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
		// task.send(new FloatPregelMessage(this.v, to, curr));
		task.sendFloat(this.v, to, curr);

	}

	public void sendAllFloat(float val) throws Exception {
		task.sendAllFloat(this.v, -1, val);

	}

	// public void finished() throws Exception {
	// MessageMerger merger = task.getConfig().getMerger();
	// if (merger != null) {
	// Collections.sort(toSend);
	// PregelMessage last = null;
	// for (PregelMessage curr : toSend) {
	// if (last == null)
	// last = curr;
	// else if (last.getTo() == curr.getTo()) {
	// Object val = merger.merge(last.getV(), curr);
	// last.setV(val);
	// } else {
	// task.send(last);
	// last = curr;
	// }
	// }
	// if (last != null)
	// task.send(last);
	// } else
	// for (PregelMessage pregelMessage : toSend) {
	// task.send(pregelMessage);
	// }
	// };
}
