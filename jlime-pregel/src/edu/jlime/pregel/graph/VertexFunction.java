package edu.jlime.pregel.graph;

import java.io.Serializable;

import edu.jlime.pregel.client.TaskContext;
import edu.jlime.pregel.worker.WorkerTask.StepData;

public interface VertexFunction extends Serializable {

	void execute(Vertex key, StepData value, TaskContext graph)
			throws Exception;

}
