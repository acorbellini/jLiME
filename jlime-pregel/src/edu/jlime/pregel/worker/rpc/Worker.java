package edu.jlime.pregel.worker.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;

public interface Worker {
	@Sync
	public void sendMessage(long from, long to, Object msg, int taskID)
			throws Exception;

	@Sync
	public void sendFloatMessage(long from, long to, float msg, int taskID)
			throws Exception;

	@Sync
	public void nextSuperstep(int superstep, int taskID,
			SplitFunction currentSplit) throws Exception;

	@Cache
	public UUID getID() throws Exception;

	public void execute(int taskID) throws Exception;

	@Sync
	void createTask(int taskID, Peer cli, VertexFunction func, long[] vList,
			PregelConfig config) throws Exception;

	@Sync
	public void cleanup(int taskID) throws Exception;

	@Sync
	public void sendBroadcastMessage(long from, Object val, int taskID)
			throws Exception;

	@Sync
	public void sendFloatBroadcastMessage(long from, float val, int taskID)
			throws Exception;
}
