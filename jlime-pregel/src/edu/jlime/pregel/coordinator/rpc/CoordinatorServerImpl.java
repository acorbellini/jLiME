package edu.jlime.pregel.coordinator.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public class CoordinatorServerImpl extends RPCClient implements Coordinator,
		Transferible {

	transient RPCDispatcher localRPC;
	transient volatile Coordinator local = null;

	public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPCDispatcher.getLocalDispatcher(dest);
	}

	public PregelExecution execute(final VertexFunction arg0,
			final long[] arg1, final PregelConfig arg2, final Peer arg3)
			throws Exception {
		if (localRPC != null) {
			return getLocal().execute(arg0, arg1, arg2, arg3);
		}
		return (PregelExecution) disp.callSync(dest, client, targetID,
				"execute", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public Double getAggregatedValue(final int arg0, final Long arg1,
			final String arg2) throws Exception {
		if (localRPC != null) {
			return getLocal().getAggregatedValue(arg0, arg1, arg2);
		}
		return (Double) disp.callSync(dest, client, targetID,
				"getAggregatedValue", new Object[] { arg0, arg1, arg2 });
	}

	public void setAggregatedValue(final int arg0, final Long arg1,
			final String arg2, final Double arg3) throws Exception {
		if (localRPC != null) {
			getLocal().setAggregatedValue(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setAggregatedValue",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void finished(final int arg0, final UUID arg1, final Boolean arg2)
			throws Exception {
		if (localRPC != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						getLocal().finished(arg0, arg1, arg2);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "finished", new Object[] { arg0,
				arg1, arg2 });
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
		this.localRPC = RPCDispatcher.getLocalDispatcher(super.dest);
	}

	public Coordinator getLocal() throws Exception {
		if (local == null) {
			synchronized (this) {
				if (local == null) {
					this.local = (Coordinator) localRPC.getTarget(targetID);
				}
			}
		}
		return this.local;
	}
}