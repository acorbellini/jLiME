package edu.jlime.graphly.server;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.ConsistentHashing;

public class GraphlyCoordinatorServerImpl extends RPCClient implements Coordinator, Transferible {

	transient RPC local = null;

	public GraphlyCoordinatorServerImpl(RPC disp, Peer dest, Peer client, String targetID) {
		super(disp, dest, client, targetID);
		local = RPC.getLocalDispatcher(dest);
	}

	public ConsistentHashing getHash() throws Exception {
		if (local != null) {
			return ((Coordinator) local.getTarget(targetID)).getHash();
		}
		return (ConsistentHashing) disp.callSync(dest, client, targetID, "getHash", new Object[] {});
	}

	@Override
	public void setRPC(RPC rpc) {
		this.disp = rpc;
	}
}