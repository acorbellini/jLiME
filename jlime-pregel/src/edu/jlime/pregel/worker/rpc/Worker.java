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

public interface Worker {

	@Sync
	public void nextSuperstep(int superstep, int taskID,
			SplitFunction currentSplit, Map<String, Aggregator> aggregator)
			throws Exception;

	@Cache
	public UUID getID() throws Exception;

	public void execute(int taskID) throws Exception;

	@Sync
	void createTask(int taskID, Peer cli, VertexFunction<?> func, long[] vList,
			PregelConfig config) throws Exception;

	@Sync
	public void cleanup(int taskID) throws Exception;

	@Sync
	public void sendMessage(String msgType, long from, long to, Object msg,
			int taskID) throws Exception;

	@Sync
	public void sendFloatMessage(String msgType, long from, long to, float msg,
			int taskID) throws Exception;

	@Sync
	public void sendBroadcastMessage(String msgType, long from, Object val,
			int taskID) throws Exception;

	@Sync
	public void sendFloatBroadcastMessage(String msgType, long from, float val,
			int taskID) throws Exception;

	@Sync
	public void sendFloatMessage(String msgType, long from, long[] keys,
			float[] values, int taskid) throws Exception;

	@Sync
	public void sendDoubleMessage(String msgType, long i, long[] keys,
			double[] values, int taskid) throws Exception;

	@Sync
	public void sendDoubleMessage(String msgType, long from, long to,
			double val, int taskid) throws Exception;

	@Sync
	public void sendDoubleBroadcastMessage(String msgType, long from,
			double val, int taskid) throws Exception;

	@Sync
	public void sendFloatArrayMessage(String msgtype, long from, long to,
			float[] value, int taskid) throws Exception;

	@Sync
	public void sendFloatArrayMessage(String msgType, long l, long[] vids,
			float[][] data, int taskid) throws Exception;

	@Sync
	public void sendFloatArrayBroadcastMessage(String msgtype, long from,
			float[] value, int taskid) throws Exception;

	@Sync
	public void sendObjectsMessage(String msgType, long[] from, long[] vids,
			Object[] objects, int taskid) throws Exception;

	@Sync
	public void sendBroadcastMessageSubgraph(String msgType, String subGraph,
			long v, Object val, int taskid) throws Exception;

	@Sync
	public void sendBroadcastMessageSubgraphFloat(String msgType,
			String subgraph, long v, float val, int taskid) throws Exception;
}
