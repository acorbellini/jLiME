package edu.jlime.pregel.coordinator;

import edu.jlime.pregel.coordinator.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;

public class CoordinatorServerImpl implements Coordinator {

	RPCDispatcher disp;
	Peer local;
	Peer dest;
	Peer client;
	String targetID;

	public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		this.disp = disp;
		this.dest = dest;
		this.client = client;
		this.targetID = targetID;
	}

	public void finished(int arg0) throws Exception {
		disp.callAsync(dest, client, targetID, "finished",
				new Object[] { arg0 });
	}

}