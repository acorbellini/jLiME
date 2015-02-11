package edu.jlime.pregel.worker;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JLiMEFactory;

public class WorkerServer {
	private RPCDispatcher disp;
	private ClientManager<Coordinator, CoordinatorBroadcast> coord;
	private ClientManager<Worker, WorkerBroadcast> workers;
	private Logger log = Logger.getLogger(WorkerServer.class);

	public WorkerServer() throws Exception {
		Configuration config = new Configuration();
		config.port = 6060;
		config.mcastport = 5050;

		HashMap<String, String> data = new HashMap<>();
		data.put("app", "graphly");
		data.put("type", "worker");

		JLiMEFactory fact = new JLiMEFactory(config, data, new DataFilter(
				"app", "graphly"));
		disp = fact.build();

		// coord = disp.manage(new CoordinatorFactory(disp, "coordinator"),
		// new PeerFilter() {
		//
		// @Override
		// public boolean verify(Peer p) {
		// return p.getData("type").equals("coordinator");
		// }
		// }, disp.getCluster().getLocalPeer());
		//
		// workers = disp.manage(new WorkerFactory(disp, "worker"),
		// new PeerFilter() {
		//
		// @Override
		// public boolean verify(Peer p) {
		// return p.getData("type").equals("worker");
		// }
		// }, disp.getCluster().getLocalPeer());

	}

	public static void main(String[] args) throws Exception {
		new WorkerServer().start();
	}

	public void start() throws Exception {
		disp.start();

		disp.registerTarget("worker", new WorkerImpl(disp), true);

		log.info("jLiME Worker Started");
	}

	public void stop() throws Exception {
		disp.stop();
	}
}
