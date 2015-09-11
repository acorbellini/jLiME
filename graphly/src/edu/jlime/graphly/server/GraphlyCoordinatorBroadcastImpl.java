package edu.jlime.graphly.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.graphly.client.ConsistentHashing;

public class GraphlyCoordinatorBroadcastImpl implements GraphlyCoordinatorBroadcast {

	RPC disp;
	Peer local;
	List<Peer> dest = new ArrayList<Peer>();
	Peer client;
	String targetID;

	public GraphlyCoordinatorBroadcastImpl(RPC disp, List<Peer> dest, Peer client, String targetID) {
		this.disp = disp;
		this.dest.addAll(dest);
		this.client = client;
		this.targetID = targetID;
	}

	public Map<Peer, ConsistentHashing> getHash() throws Exception {
		return disp.multiCall(dest, client, targetID, "getHash", new Object[] {});
	}

}