package edu.jlime.jd.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.jd.JobContainer;
import edu.jlime.jd.Node;

public class JobExecutorServerImpl extends RPCClient implements JobExecutor, Transferible {

	transient RPC localRPC;
	transient volatile JobExecutor local = null;

	public JobExecutorServerImpl(RPC disp, Peer dest, Peer client, String targetID) {
		super(disp, dest, client, targetID);
		this.localRPC = RPC.getLocalDispatcher(dest);
	}

	public void execute(final JobContainer arg0) throws Exception {

		async.execute(new Runnable() {
			public void run() {
				if (localRPC != null) {
					try {
						getLocal().execute(arg0);
					} catch (Exception e) {
						e.printStackTrace();
					}
				} else
					try {
						disp.callAsync(dest, client, targetID, "execute", new Object[] { arg0 });
					} catch (Exception e) {
						e.printStackTrace();
					}
			}
		});
	}

	public void result(final Object arg0, final UUID arg1, final Node arg2) throws Exception {
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
		disp.callAsync(dest, client, targetID, "result", new Object[] { arg0, arg1, arg2 });
	}

	@Override
	public void setRPC(RPC rpc) {
		this.disp = rpc;
		this.localRPC = RPC.getLocalDispatcher(super.dest);
	}

	public JobExecutor getLocal() throws Exception {
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