package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.HashSet;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.worker.PregelMessage;

public interface VertexFunction extends Serializable {

	void execute(Vertex v, HashSet<PregelMessage> in, WorkerContext ctx)
			throws Exception;

}
