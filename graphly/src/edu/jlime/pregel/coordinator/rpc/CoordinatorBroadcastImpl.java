package edu.jlime.pregel.coordinator.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public class CoordinatorBroadcastImpl implements CoordinatorBroadcast {

	RPC disp;
	Peer local;
	List<Peer> dest = new ArrayList<Peer>();
	Peer client;
	String targetID;

	public CoordinatorBroadcastImpl(RPC disp, List<Peer> dest, Peer client, String targetID) {
		this.disp = disp;
		this.dest.addAll(dest);
		this.client = client;
		this.targetID = targetID;
	}

	public Map<Peer, PregelExecution> execute(final VertexFunction<?> arg0, final long[] arg1, final PregelConfig arg2,
			final Peer arg3) throws Exception {
		return disp.multiCall(dest, client, targetID, "execute", new Object[] { arg0, arg1, arg2, arg3 });
	}

	public void finished(final int arg0, final UUID arg1, final Boolean arg2,
			final Map<java.lang.String, edu.jlime.pregel.coordinator.Aggregator> arg3) throws Exception {
		disp.multiCallAsync(dest, client, targetID, "finished", new Object[] { arg0, arg1, arg2, arg3 });
	}

}