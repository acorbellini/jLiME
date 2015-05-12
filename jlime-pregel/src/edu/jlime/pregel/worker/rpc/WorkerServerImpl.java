package edu.jlime.pregel.worker.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;

public class WorkerServerImpl extends RPCClient implements Worker, Transferible {

	UUID getIDCached = null;
	transient RPCDispatcher localRPC;
	transient volatile Worker local = null;

	public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPCDispatcher.getLocalDispatcher(dest);
	}

	public void execute(final int arg0) throws Exception {
		if (localRPC != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						getLocal().execute(arg0);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "execute", new Object[] { arg0 });
	}

	public void cleanup(final int arg0) throws Exception {
		if (localRPC != null) {
			getLocal().cleanup(arg0);
			return;
		}
		disp.callSync(dest, client, targetID, "cleanup", new Object[] { arg0 });
	}

	public UUID getID() throws Exception {
		if (localRPC != null) {
			return getLocal().getID();
		}
		if (getIDCached == null) {
			synchronized (this) {
				if (getIDCached == null)
					getIDCached = (UUID) disp.callSync(dest, client, targetID,
							"getID", new Object[] {});

			}
		}
		return getIDCached;
	}

	public void sendBroadcastMessage(final long arg0, final Object arg1,
			final int arg2) throws Exception {
		if (localRPC != null) {
			getLocal().sendBroadcastMessage(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "sendBroadcastMessage",
				new Object[] { arg0, arg1, arg2 });
	}

	public void sendFloatBroadcastMessage(final long arg0, final float arg1,
			final int arg2) throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatBroadcastMessage(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatBroadcastMessage",
				new Object[] { arg0, arg1, arg2 });
	}

	public void sendDoubleMessage(final long arg0, final long arg1,
			final double arg2, final int arg3) throws Exception {
		if (localRPC != null) {
			getLocal().sendDoubleMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendDoubleMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendDoubleMessage(final long arg0, final long[] arg1,
			final double[] arg2, final int arg3) throws Exception {
		if (localRPC != null) {
			getLocal().sendDoubleMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendDoubleMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendDoubleBroadcastMessage(final long arg0, final double arg1,
			final int arg2) throws Exception {
		if (localRPC != null) {
			getLocal().sendDoubleBroadcastMessage(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "sendDoubleBroadcastMessage",
				new Object[] { arg0, arg1, arg2 });
	}

	public void sendMessage(final long arg0, final long arg1,
			final Object arg2, final int arg3) throws Exception {
		if (localRPC != null) {
			getLocal().sendMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendMessage", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

	public void sendFloatMessage(final long arg0, final long arg1,
			final float arg2, final int arg3) throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatMessage", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

	public void sendFloatMessage(final long arg0, final long[] arg1,
			final float[] arg2, final int arg3) throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatMessage", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

	public void nextSuperstep(final int arg0, final int arg1,
			final SplitFunction arg2) throws Exception {
		if (localRPC != null) {
			getLocal().nextSuperstep(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "nextSuperstep", new Object[] {
				arg0, arg1, arg2 });
	}

	public void createTask(final int arg0, final Peer arg1,
			final VertexFunction arg2, final long[] arg3,
			final PregelConfig arg4) throws Exception {
		if (localRPC != null) {
			getLocal().createTask(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "createTask", new Object[] {
				arg0, arg1, arg2, arg3, arg4 });
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
		this.localRPC = RPCDispatcher.getLocalDispatcher(super.dest);
	}

	public Worker getLocal() throws Exception {
		if (local == null) {
			synchronized (this) {
				if (local == null) {
					this.local = (Worker) localRPC.getTarget(targetID);
				}
			}
		}
		return this.local;
	}
}