package edu.jlime.pregel.worker.rpc;

import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.client.SplitFunction;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;

public class WorkerServerImpl extends RPCClient implements Worker, Transferible {

	UUID getIDCached = null;
	transient Worker local = null;

	public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		RPCDispatcher localRPC = RPCDispatcher.getLocalDispatcher(dest);
		if (localRPC != null)
			this.local = (Worker) localRPC.getTarget(targetID);
	}

	public void execute(final UUID arg0) throws Exception {
		if (local != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						local.execute(arg0);
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

	public UUID getID() throws Exception {
		if (local != null) {
			return local.getID();
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

	public void nextSuperstep(final int arg0, final UUID arg1,
			final SplitFunction arg2) throws Exception {
		if (local != null) {
			local.nextSuperstep(arg0, arg1, arg2);
			return;
		}
		disp.callSync(dest, client, targetID, "nextSuperstep", new Object[] {
				arg0, arg1, arg2 });
	}

	public void sendMessage(final PregelMessage arg0, final UUID arg1)
			throws Exception {
		if (local != null) {
			local.sendMessage(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "sendMessage", new Object[] {
				arg0, arg1 });
	}

	public void sendMessages(
			final List<edu.jlime.pregel.worker.PregelMessage> arg0,
			final UUID arg1) throws Exception {
		if (local != null) {
			local.sendMessages(arg0, arg1);
			return;
		}
		disp.callSync(dest, client, targetID, "sendMessages", new Object[] {
				arg0, arg1 });
	}

	public void createTask(final UUID arg0, final Peer arg1,
			final VertexFunction arg2, final PregelConfig arg3)
			throws Exception {
		if (local != null) {
			local.createTask(arg0, arg1, arg2, arg3);
			return;
		}
		disp.callSync(dest, client, targetID, "createTask", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
	}
}