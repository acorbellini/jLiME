package edu.jlime.pregel.client;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.worker.VertexData;
import edu.jlime.pregel.worker.WorkerTask;

public class WorkerContext {
	private WorkerTask task;
	private Vertex v;

	public WorkerContext(WorkerTask task, Vertex v) {
		this.task = task;
		this.v = v;
	}

	public void send(Vertex to, VertexData data) throws Exception {
		task.send(v, to, data);
	};

	public PregelGraph getGraph() {
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
	};
}
