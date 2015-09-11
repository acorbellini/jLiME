package edu.jlime.pregel.coordinator.rpc;

import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public class CoordinatorServerImpl extends RPCClient implements Coordinator, Transferible {

	transient RPC localRPC;
	transient volatile Coordinator local = null;

	public CoordinatorServerImpl(RPC disp, Peer dest, Peer client, String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPC.getLocalDispatcher(dest);
	}

	public PregelExecution execute(final VertexFunction<?> arg0, final long[] arg1, final PregelConfig arg2,
			final Peer arg3) throws Exception {
		if (localRPC != null) {
			return getLocal().execute(arg0, arg1, arg2, arg3);
		}
		return (PregelExecution) disp.callSync(dest, client, targetID, "execute",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void finished(final int arg0, final UUID arg1, final Boolean arg2,
			final Map<java.lang.String, edu.jlime.pregel.coordinator.Aggregator> arg3) throws Exception {
		if (localRPC != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						getLocal().finished(arg0, arg1, arg2, arg3);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "finished", new Object[] { arg0, arg1, arg2, arg3 });
	}

	@Override
	public void setRPC(RPC rpc) {
		this.disp = rpc;
		this.localRPC = RPC.getLocalDispatcher(super.dest);
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