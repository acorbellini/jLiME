package edu.jlime.pregel.coordinator.rpc;

import java.util.HashMap;
import java.util.UUID;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.VertexData;

public interface Coordinator {
	public void finished(UUID taskID, UUID workerID) throws Exception;

	public PregelGraph execute(PregelGraph input,
			HashMap<Vertex, VertexData> data, VertexFunction func,
			Integer supersteps) throws Exception;
}
