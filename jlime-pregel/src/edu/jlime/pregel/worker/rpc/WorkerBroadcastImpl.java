package edu.jlime.pregel.worker.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;

public class WorkerBroadcastImpl implements WorkerBroadcast {

	RPCDispatcher disp;
	Peer local;
	List<Peer> dest = new ArrayList<Peer>();
	Peer client;
	String targetID;

	public WorkerBroadcastImpl(RPCDispatcher disp, List<Peer> dest,
			Peer client, String targetID) {
		this.disp = disp;
		this.dest.addAll(dest);
		this.client = client;
		this.targetID = targetID;
	}

	public void execute(final int arg0) throws Exception {
		disp.multiCallAsync(dest, client, targetID, "execute",
				new Object[] { arg0 });
	}

	public void cleanup(final int arg0) throws Exception {
		disp.multiCall(dest, client, targetID, "cleanup", new Object[] { arg0 });
	}

	public Map<Peer, UUID> getID() throws Exception {
		return disp.multiCall(dest, client, targetID, "getID", new Object[] {});
	}

	public void sendBroadcastMessage(final long arg0, final Object arg1,
			final int arg2) throws Exception {
		disp.multiCall(dest, client, targetID, "sendBroadcastMessage",
				new Object[] { arg0, arg1, arg2 });
	}

	public void sendFloatBroadcastMessage(final long arg0, final float arg1,
			final int arg2) throws Exception {
		disp.multiCall(dest, client, targetID, "sendFloatBroadcastMessage",
				new Object[] { arg0, arg1, arg2 });
	}

	public void sendDoubleMessage(final long arg0, final long arg1,
			final double arg2, final int arg3) throws Exception {
		disp.multiCall(dest, client, targetID, "sendDoubleMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendDoubleMessage(final long arg0, final long[] arg1,
			final double[] arg2, final int arg3) throws Exception {
		disp.multiCall(dest, client, targetID, "sendDoubleMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendDoubleBroadcastMessage(final long arg0, final double arg1,
			final int arg2) throws Exception {
		disp.multiCall(dest, client, targetID, "sendDoubleBroadcastMessage",
				new Object[] { arg0, arg1, arg2 });
	}

	public void sendMessage(final long arg0, final long arg1,
			final Object arg2, final int arg3) throws Exception {
		disp.multiCall(dest, client, targetID, "sendMessage", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

	public void sendFloatMessage(final long arg0, final long arg1,
			final float arg2, final int arg3) throws Exception {
		disp.multiCall(dest, client, targetID, "sendFloatMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendFloatMessage(final long arg0, final long[] arg1,
			final float[] arg2, final int arg3) throws Exception {
		disp.multiCall(dest, client, targetID, "sendFloatMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void nextSuperstep(final int arg0, final int arg1,
			final SplitFunction arg2) throws Exception {
		disp.multiCall(dest, client, targetID, "nextSuperstep", new Object[] {
				arg0, arg1, arg2 });
	}

	public void createTask(final int arg0, final Peer arg1,
			final VertexFunction arg2, final long[] arg3,
			final PregelConfig arg4) throws Exception {
		disp.multiCall(dest, client, targetID, "createTask", new Object[] {
				arg0, arg1, arg2, arg3, arg4 });
	}

}