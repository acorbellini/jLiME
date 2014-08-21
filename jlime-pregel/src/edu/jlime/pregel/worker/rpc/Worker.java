package edu.jlime.pregel.worker.rpc;

import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface Worker {
	public void sendDataToVertex(Vertex vertexid, byte[] data) throws Exception;

	public void nextSuperstep(int superstep) throws Exception;

	public void schedule(Vertex vertexid, VertexFunction vertex)
			throws Exception;

	@Sync
	public void setGraph(PregelGraph input) throws Exception;
}
