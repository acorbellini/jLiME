package edu.jlime.pregel.worker.rpc;

import java.util.HashMap;
import java.util.UUID;

import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.VertexData;

public interface Worker {
	@Sync
	public void sendDataToVertex(Vertex from, Vertex to, VertexData data,
			UUID taskID) throws Exception;

	public void nextSuperstep(Integer superstep, UUID taskID) throws Exception;

	@Sync
	public void createTask(PregelGraph input, VertexFunction func, UUID taskID,
			HashMap<Vertex, VertexData> initialData) throws Exception;

	@Cache
	public UUID getID() throws Exception;

	public boolean hasWork(UUID taskID) throws Exception;

	public PregelGraph getResult(UUID taskID) throws Exception;

}
