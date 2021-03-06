package edu.jlime.pregel.worker.rpc;

import java.util.Map;
import java.util.Map;
import java.util.UUID;
import java.util.UUID;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;

public interface WorkerBroadcast {

	public void execute(final int arg0) throws Exception;

	public void cleanup(final int arg0) throws Exception;

	public Map<Peer, UUID> getID() throws Exception;

	public void sendBroadcastMessageSubgraphFloat(final String arg0,
			final String arg1, final long arg2, final float arg3,
			final int arg4) throws Exception;

	public void sendFloatBroadcastMessage(final String arg0, final long arg1,
			final float arg2, final int arg3) throws Exception;

	public void createTask(final int arg0, final Peer arg1,
			final VertexFunction<edu.jlime.pregel.messages.PregelMessage> arg2,
			final long[] arg3, final PregelConfig arg4) throws Exception;

	public void nextSuperstep(final int arg0, final int arg1,
			final SplitFunction arg2,
			final Map<java.lang.String, edu.jlime.pregel.coordinator.Aggregator> arg3)
					throws Exception;

	public void sendFloatMessage(final UUID arg0, final String arg1,
			final long arg2, final long[] arg3, final float[] arg4,
			final int arg5) throws Exception;

	public void sendFloatMessage(final UUID arg0, final String arg1,
			final long arg2, final long arg3, final float arg4, final int arg5)
					throws Exception;

}