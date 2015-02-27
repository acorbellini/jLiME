package edu.jlime.graphly.server;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCDispatcher.RPCStatus;
import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.graphly.GraphlyStoreNodeIBroadcast;
import edu.jlime.graphly.GraphlyStoreNodeIFactory;
import edu.jlime.graphly.client.ConsistentHashing;

public class GraphlyCoordinatorImpl implements GraphlyCoordinator {

	public static final String COORDINATOR = "coordinator";

	private ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr;
	private ConsistentHashing hash;
	private RPCDispatcher rpc;

	public GraphlyCoordinatorImpl(RPCDispatcher rpc, int nodes)
			throws Exception {
		// this.rpc = new JLiMEFactory(DataUtil.map("app:graphly",
		// "type:coord"),
		// new DataFilter("app", "graphly")).build();

		rpc.registerTarget("Coordinator", this, true, RPCStatus.INIT);

		this.mgr = rpc.manage(new GraphlyStoreNodeIFactory(rpc, "graphly"),
				new DataFilter("app", "graphly-server", true), rpc.getCluster()
						.getLocalPeer());

		this.mgr.waitForClient(nodes);

		this.hash = new ConsistentHashing(mgr.getMap(), 32);

		rpc.setTargetsStatuses("Coordinator", RPCStatus.STARTED);
	}

	public ConsistentHashing getHash() {
		return this.hash;
	}

	public void stop() throws Exception {
		this.rpc.stop();
	}
}