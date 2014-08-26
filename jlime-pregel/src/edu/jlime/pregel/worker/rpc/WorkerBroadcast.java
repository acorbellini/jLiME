package edu.jlime.pregel.worker.rpc;

import java.util.HashSet;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.VertexData;

public interface WorkerBroadcast {

	public Map<Peer, PregelGraph> getResult(UUID arg0) throws Exception;

	public Map<Peer, UUID> getID() throws Exception;

	public void sendDataToVertex(Vertex arg0, Vertex arg1, VertexData arg2,
			UUID arg3) throws Exception;

	public void nextSuperstep(Integer arg0, UUID arg1) throws Exception;

	public void createTask(PregelGraph arg0, VertexFunction arg1, UUID arg2,
			HashSet<edu.jlime.pregel.graph.Vertex> arg3) throws Exception;

}