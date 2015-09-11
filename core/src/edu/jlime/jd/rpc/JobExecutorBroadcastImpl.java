package edu.jlime.jd.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.jd.JobContainer;
import edu.jlime.jd.Node;

public class JobExecutorBroadcastImpl implements JobExecutorBroadcast {

	RPC disp;
	Peer local;
	List<Peer> dest = new ArrayList<Peer>();
	Peer client;
	String targetID;

	public JobExecutorBroadcastImpl(RPC disp, List<Peer> dest, Peer client, String targetID) {
		this.disp = disp;
		this.dest.addAll(dest);
		this.client = client;
		this.targetID = targetID;
	}

	public void execute(final JobContainer arg0) throws Exception {
		disp.multiCallAsync(dest, client, targetID, "execute", new Object[] { arg0 });
	}

	public void result(final Object arg0, final UUID arg1, final Node arg2) throws Exception {
		disp.multiCallAsync(dest, client, targetID, "result", new Object[] { arg0, arg1, arg2 });
	}

}