package edu.jlime.pregel.worker.rpc;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;

public interface WorkerBroadcast {

	public void execute(final UUID arg0) throws Exception;

	public Map<Peer, UUID> getID() throws Exception;

	public void nextSuperstep(final int arg0, final UUID arg1,
			final SplitFunction arg2) throws Exception;

	public void sendMessage(final PregelMessage arg0, final UUID arg1)
			throws Exception;

	public void sendMessages(
			final List<edu.jlime.pregel.worker.PregelMessage> arg0,
			final UUID arg1) throws Exception;

	public void createTask(final UUID arg0, final Peer arg1,
			final VertexFunction arg2, final PregelConfig arg3)
			throws Exception;

}