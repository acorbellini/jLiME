package edu.jlime.pregel.coordinator.rpc;

import java.util.List;
import java.util.UUID;

import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {
	public void finished(UUID workerID) throws Exception;

	public PregelGraph execute(List<Vertex> vertex, VertexFunction func)
			throws Exception;

	@Sync
	public void setGraph(PregelGraph input) throws Exception;
}
