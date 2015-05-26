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

	public void sendObjectsMessage(final String arg0, final long arg1,
			final long[] arg2, final Object[] arg3, final int arg4)
			throws Exception;

	public void sendFloatArrayBroadcastMessage(final String arg0,
			final long arg1, final float[] arg2, final int arg3)
			throws Exception;

	public void sendFloatArrayMessage(final String arg0, final long arg1,
			final long arg2, final float[] arg3, final int arg4)
			throws Exception;

	public void sendFloatArrayMessage(final String arg0, final long arg1,
			final long[] arg2, final float[][] arg3, final int arg4)
			throws Exception;

	public void sendBroadcastMessage(final String arg0, final long arg1,
			final Object arg2, final int arg3) throws Exception;

	public void sendFloatBroadcastMessage(final String arg0, final long arg1,
			final float arg2, final int arg3) throws Exception;

	public void sendDoubleMessage(final String arg0, final long arg1,
			final long arg2, final double arg3, final int arg4)
			throws Exception;

	public void sendDoubleMessage(final String arg0, final long arg1,
			final long[] arg2, final double[] arg3, final int arg4)
			throws Exception;

	public void sendDoubleBroadcastMessage(final String arg0, final long arg1,
			final double arg2, final int arg3) throws Exception;

	public void createTask(final int arg0, final Peer arg1,
			final VertexFunction<?> arg2, final long[] arg3,
			final PregelConfig arg4) throws Exception;

	public void sendMessage(final String arg0, final long arg1,
			final long arg2, final Object arg3, final int arg4)
			throws Exception;

	public void sendFloatMessage(final String arg0, final long arg1,
			final long arg2, final float arg3, final int arg4) throws Exception;

	public void sendFloatMessage(final String arg0, final long arg1,
			final long[] arg2, final float[] arg3, final int arg4)
			throws Exception;

	public void nextSuperstep(final int arg0, final int arg1,
			final SplitFunction arg2) throws Exception;

}