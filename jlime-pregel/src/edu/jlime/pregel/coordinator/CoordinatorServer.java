package edu.jlime.pregel.coordinator;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.rpc.NetworkConfiguration;

public class CoordinatorServer {
	public static final String COORDINATOR_KEY = "pregel_coordinator";
	private ClientManager<Worker, WorkerBroadcast> workers;
	private Logger log = Logger.getLogger(CoordinatorServer.class);

	public CoordinatorServer(RPCDispatcher rpc) throws Exception {

		this.workers = rpc.manage(new WorkerFactory(rpc,
				WorkerServer.WORKER_KEY), new PeerFilter() {
			public boolean verify(Peer p) {
				if (p.getData("app").contains(WorkerServer.WORKER_KEY))
					return true;
				return false;
			}
		}, rpc.getCluster().getLocalPeer());
		rpc.registerTarget(COORDINATOR_KEY, new CoordinatorImpl(rpc, workers),
				true);

	}

	public static void main(String[] args) throws Exception {
		CoordinatorServer.build();
	}

	private static CoordinatorServer build() throws Exception {
		NetworkConfiguration config = new NetworkConfiguration();
		config.port = 6070;
		config.mcastport = 5050;

		HashMap<String, String> data = new HashMap<>();
		data.put("app", "pregel");
		data.put("type", COORDINATOR_KEY);

		RPCDispatcher rpc = new JLiMEFactory(config, data, new PeerFilter() {

			@Override
			public boolean verify(Peer p) {
				String data = p.getData("app");
				return (data != null && data.equals("pregel"));
			}
		}).build();

		CoordinatorServer coord = new CoordinatorServer(rpc);

		rpc.start();

		return coord;

	}
}
