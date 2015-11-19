package edu.jlime.graphly.server;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.Client;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.RPC.RPCStatus;
import edu.jlime.graphly.client.ConsistentHashing;
import edu.jlime.graphly.storenode.rpc.StoreNode;
import edu.jlime.graphly.storenode.rpc.StoreNodeBroadcast;
import edu.jlime.graphly.storenode.rpc.StoreNodeFactory;

public class GraphlyCoordinatorImpl implements Coordinator {

	public static final String COORDINATOR = "coordinator";

	private Client<StoreNode, StoreNodeBroadcast> mgr;
	private ConsistentHashing hash;

	public GraphlyCoordinatorImpl(RPC rpc, int nodes) throws Exception {
		// this.rpc = new JLiMEFactory(DataUtil.map("app:graphly",
		// "type:coord"),
		// new DataFilter("app", "graphly")).build();

		rpc.registerTarget("Coordinator", this, true, RPCStatus.INIT);

		this.mgr = rpc.manage(new StoreNodeFactory(rpc, "graphly"),
				new DataFilter("app", "graphly-server", true),
				rpc.getCluster().getLocalPeer());

		this.mgr.waitForClient(nodes);

		this.hash = new ConsistentHashing(mgr.getMap(), 2048);

		rpc.setTargetsStatuses("Coordinator", RPCStatus.STARTED);
	}

	public ConsistentHashing getHash() {
		return this.hash;
	}

	public void stop() {
	}
}