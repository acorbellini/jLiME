package edu.jlime.pregel.coordinator;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class CoordinatorServer {
	private RPCDispatcher rpc;
	private ClientManager<Worker, WorkerBroadcast> workers;
	private Logger log = Logger.getLogger(CoordinatorServer.class);

	public CoordinatorServer() throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;

		HashMap<String, String> data = new HashMap<>();
		data.put("type", "coordinator");

		this.rpc = new JlimeFactory(config, data).build();

		this.workers = rpc.manage(new WorkerFactory(rpc, "worker"),
				new PeerFilter() {
					public boolean verify(Peer p) {
						if (p.getData("type").equals("worker"))
							return true;
						return false;
					}
				});
		this.rpc.registerTarget("coordinator", new CoordinatorImpl(workers));

	}

	public void start() throws Exception {

		this.rpc.start();

		log.info("jLiME Pregel Coordinator Started");
	}

	public void stop() throws Exception {
		this.rpc.stop();
	}
}
