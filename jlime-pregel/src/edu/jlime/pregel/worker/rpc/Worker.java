package edu.jlime.pregel.worker.rpc;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;

public interface Worker {
	@Sync
	public void sendMessage(PregelMessage msg, UUID taskID) throws Exception;

	@Sync
	public void nextSuperstep(int superstep, UUID taskID, SplitFunction currentSplit) throws Exception;

	@Cache
	public UUID getID() throws Exception;

	public void execute(UUID taskID) throws Exception;

	@Sync
	void createTask(UUID taskID, Peer cli, VertexFunction func,
			PregelConfig config) throws Exception;

	@Sync
	public void sendMessages(List<PregelMessage> value, UUID taskid)
			throws Exception;

}
