package edu.jlime.pregel.worker.rpc;

import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.lang.Exception;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.client.PregelConfig;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.pregel.client.SplitFunction;
import java.util.Map;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;

public class WorkerServerImpl extends RPCClient implements Worker, Transferible {

	UUID getIDCached = null;
	transient RPC localRPC;
	transient volatile Worker local = null;

	public WorkerServerImpl(RPC disp, Peer dest, Peer client, String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPC.getLocalDispatcher(dest);
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
					getIDCached = (UUID) disp.callSync(dest, client, targetID, "getID", new Object[] {});

			}
		}
		return getIDCached;
	}

	public void sendBroadcastMessageSubgraphFloat(final String arg0, final String arg1, final long arg2,
			final float arg3, final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendBroadcastMessageSubgraphFloat(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendBroadcastMessageSubgraphFloat",
				new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void createTask(final int arg0, final Peer arg1,
			final VertexFunction<edu.jlime.pregel.messages.PregelMessage> arg2, final long[] arg3,
			final PregelConfig arg4) throws Exception {
		if (localRPC != null) {
			getLocal().createTask(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "createTask", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendFloatMessage(final String arg0, final long arg1, final long[] arg2, final float[] arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendFloatMessage(final String arg0, final long arg1, final long arg2, final float arg3, final int arg4)
			throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void nextSuperstep(final int arg0, final int arg1, final SplitFunction arg2,
			final Map<java.lang.String, edu.jlime.pregel.coordinator.Aggregator> arg3) throws Exception {
		if (localRPC != null) {
			getLocal().nextSuperstep(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "nextSuperstep", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendMessage(final String arg0, final long arg1, final long arg2, final Object arg3, final int arg4)
			throws Exception {
		if (localRPC != null) {
			getLocal().sendMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendFloatArrayBroadcastMessage(final String arg0, final long arg1, final float[] arg2, final int arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatArrayBroadcastMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatArrayBroadcastMessage",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendDoubleBroadcastMessage(final String arg0, final long arg1, final double arg2, final int arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().sendDoubleBroadcastMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendDoubleBroadcastMessage", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendBroadcastMessage(final String arg0, final long arg1, final Object arg2, final int arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().sendBroadcastMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendBroadcastMessage", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendFloatArrayMessage(final String arg0, final long arg1, final long[] arg2, final float[][] arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatArrayMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatArrayMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendFloatArrayMessage(final String arg0, final long arg1, final long arg2, final float[] arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatArrayMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatArrayMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendObjectsMessage(final String arg0, final long[] arg1, final long[] arg2, final Object[] arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendObjectsMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendObjectsMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendDoubleMessage(final String arg0, final long arg1, final long arg2, final double arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendDoubleMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendDoubleMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendDoubleMessage(final String arg0, final long arg1, final long[] arg2, final double[] arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendDoubleMessage(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendDoubleMessage", new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	public void sendFloatBroadcastMessage(final String arg0, final long arg1, final float arg2, final int arg3)
			throws Exception {
		if (localRPC != null) {
			getLocal().sendFloatBroadcastMessage(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "sendFloatBroadcastMessage", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void sendBroadcastMessageSubgraph(final String arg0, final String arg1, final long arg2, final Object arg3,
			final int arg4) throws Exception {
		if (localRPC != null) {
			getLocal().sendBroadcastMessageSubgraph(arg0, arg1, arg2, arg3, arg4);
			return;
		}
		disp.callSync(dest, client, targetID, "sendBroadcastMessageSubgraph",
				new Object[] { arg0, arg1, arg2, arg3, arg4 });
	}

	@Override
	public void setRPC(RPC rpc) {
		this.disp = rpc;
		this.localRPC = RPC.getLocalDispatcher(super.dest);
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