package edu.jlime.pregel.worker.rpc;

import java.util.HashMap;
import java.util.UUID;

import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface Worker {
	public void sendDataToVertex(Vertex from, Vertex to, byte[] data,
			UUID taskID) throws Exception;

	public void nextSuperstep(int superstep, UUID taskID) throws Exception;

	@Sync
	public void createTask(PregelGraph input, VertexFunction func, UUID taskID,
			HashMap<Vertex, byte[]> initialData) throws Exception;

	@Cache
	public UUID getID() throws Exception;

	public boolean hasWork(UUID taskID) throws Exception;

	public PregelGraph getResult(UUID taskID) throws Exception;

}
