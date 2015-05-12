package edu.jlime.pregel.worker.rpc;

import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;

public interface WorkerBroadcast {

	public void execute(final int arg0) throws Exception;

	public void cleanup(final int arg0) throws Exception;

	public Map<Peer, UUID> getID() throws Exception;

	public void sendBroadcastMessage(final long arg0, final Object arg1,
			final int arg2) throws Exception;

	public void sendFloatBroadcastMessage(final long arg0, final float arg1,
			final int arg2) throws Exception;

	public void sendDoubleMessage(final long arg0, final long arg1,
			final double arg2, final int arg3) throws Exception;

	public void sendDoubleMessage(final long arg0, final long[] arg1,
			final double[] arg2, final int arg3) throws Exception;

	public void sendDoubleBroadcastMessage(final long arg0, final double arg1,
			final int arg2) throws Exception;

	public void sendMessage(final long arg0, final long arg1,
			final Object arg2, final int arg3) throws Exception;

	public void sendFloatMessage(final long arg0, final long arg1,
			final float arg2, final int arg3) throws Exception;

	public void sendFloatMessage(final long arg0, final long[] arg1,
			final float[] arg2, final int arg3) throws Exception;

	public void nextSuperstep(final int arg0, final int arg1,
			final SplitFunction arg2) throws Exception;

	public void createTask(final int arg0, final Peer arg1,
			final VertexFunction arg2, final long[] arg3,
			final PregelConfig arg4) throws Exception;

}