package edu.jlime.jd.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobContainer;

public class JobExecutorServerImpl extends RPCClient implements JobExecutor,
		Transferible {

	transient RPCDispatcher localRPC;
	transient volatile JobExecutor local = null;

	public JobExecutorServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPCDispatcher.getLocalDispatcher(dest);
	}

	public void execute(final JobContainer arg0) throws Exception {
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

	public void result(final Object arg0, final UUID arg1, final ClientNode arg2)
			throws Exception {
		if (localRPC != null) {
			async.execute(new Runnable() {
				public void run() {
					try {
						getLocal().result(arg0, arg1, arg2);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
			;
			return;
		}
		disp.callAsync(dest, client, targetID, "result", new Object[] { arg0,
				arg1, arg2 });
	}

	@Override
	public void setRPC(RPCDispatcher rpc) {
		this.disp = rpc;
		this.localRPC = RPCDispatcher.getLocalDispatcher(super.dest);
	}

	public JobExecutor getLocal() {
		if (local == null) {
			synchronized (this) {
				if (local == null) {
					this.local = (JobExecutor) localRPC.getTarget(targetID);
				}
			}
		}
		return this.local;
	}
}