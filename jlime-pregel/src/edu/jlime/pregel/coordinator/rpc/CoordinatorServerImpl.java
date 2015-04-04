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

	transient Coordinator local = null;

	public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		RPCDispatcher localRPC = RPCDispatcher.getLocalDispatcher(dest);
		if (localRPC != null)
			this.local = (Coordinator) localRPC.getTarget(targetID);
	}

	public PregelExecution execute(final VertexFunction arg0,
			final PregelConfig arg1, final Peer arg2) throws Exception {
		if (local != null) {
			return local.execute(arg0, arg1, arg2);
		}
		return (PregelExecution) disp.callSync(dest, client, targetID,
				"execute", new Object[] { arg0, arg1, arg2 });
	}

	public void setAggregatedValue(final UUID arg0, final Long arg1,
			final String arg2, final Double arg3) throws Exception {
		if (local != null) {
			local.setAggregatedValue(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "setAggregatedValue",
				new Object[] { arg0, arg1, arg2, arg3 });
	}

	public Double getAggregatedValue(final UUID arg0, final Long arg1,
			final String arg2) throws Exception {
		if (local != null) {
			return local.getAggregatedValue(arg0, arg1, arg2);
		}
		return (Double) disp.callSync(dest, client, targetID,
				"getAggregatedValue", new Object[] { arg0, arg1, arg2 });
	}

	public void finished(final UUID arg0, final UUID arg1, final Boolean arg2)
			throws Exception {
		if (local != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						local.finished(arg0, arg1, arg2);
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
	}
}