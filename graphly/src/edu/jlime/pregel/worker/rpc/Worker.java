package edu.jlime.pregel.worker.rpc;

import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.Sync;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.messages.PregelMessage;

public interface Worker {

	@Sync
	public void nextSuperstep(int superstep, int taskID, SplitFunction currentSplit, Map<String, Aggregator> aggregator)
			throws Exception;

	@Cache
	public UUID getID() throws Exception;

	public void execute(int taskID) throws Exception;

	@Sync
	void createTask(int taskID, Peer cli, VertexFunction<PregelMessage> func, long[] vList, PregelConfig config)
			throws Exception;

	@Sync
	public void cleanup(int taskID) throws Exception;

	@Sync
	public void sendFloatMessage(String msgType, long from, long to, float msg, int taskID) throws Exception;

	@Sync
	public void sendFloatBroadcastMessage(String msgType, long from, float val, int taskID) throws Exception;

	@Sync
	public void sendFloatMessage(String msgType, long from, long[] keys, float[] values, int taskid) throws Exception;

	@Sync
	public void sendBroadcastMessageSubgraphFloat(String msgType, String subgraph, long v, float val, int taskid)
			throws Exception;
	//
	// @Sync
	// public void finishedProcessing(int taskID) throws Exception;
}
