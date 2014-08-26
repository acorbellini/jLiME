package edu.jlime.pregel.client;

import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.worker.VertexData;

public interface WorkerContext {
	public PregelGraph getGraph();

	public void send(Vertex from, Vertex to, VertexData data) throws Exception;

	public void setHalted(Vertex v);
}
