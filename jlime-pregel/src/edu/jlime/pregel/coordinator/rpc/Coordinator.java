package edu.jlime.pregel.coordinator.rpc;

import java.util.HashMap;
import java.util.UUID;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface Coordinator {
	public void finished(UUID taskID, UUID workerID) throws Exception;

	public PregelGraph execute(PregelGraph input, HashMap<Vertex, byte[]> data,
			VertexFunction func, int supersteps) throws Exception;
}
