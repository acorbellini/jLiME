package edu.jlime.pregel.worker.rpc;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;

public interface WorkerBroadcast {

	public void execute(UUID arg0) throws Exception;

	public Map<Peer, UUID> getID() throws Exception;

	public void sendMessages(List<edu.jlime.pregel.worker.PregelMessage> arg0,
			UUID arg1) throws Exception;

	public void sendMessage(PregelMessage arg0, UUID arg1) throws Exception;

	public void nextSuperstep(Integer arg0, UUID arg1) throws Exception;

	public void createTask(UUID arg0, Peer arg1, VertexFunction arg2,
			PregelConfig arg3, Set<java.lang.Long> arg4) throws Exception;

}