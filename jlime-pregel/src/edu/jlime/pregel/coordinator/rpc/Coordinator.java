package edu.jlime.pregel.coordinator.rpc;

import java.util.List;
import java.util.UUID;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {
	public void finished(UUID taskID, UUID workerID, Boolean processedWork)
			throws Exception;

	public PregelGraph execute(PregelGraph input, VertexFunction minTree,
			List<Vertex> vList, Integer supersteps) throws Exception;
}
