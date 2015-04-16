package edu.jlime.pregel.graph;

import java.io.Serializable;
import java.util.List;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.worker.PregelMessage;

public interface VertexFunction extends Serializable {
	void execute(long v, List<PregelMessage> in, WorkerContext ctx)
			throws Exception;
}
