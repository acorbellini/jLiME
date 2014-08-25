package edu.jlime.pregel.worker;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class WorkerServer {
	private RPCDispatcher disp;
	private ClientManager<Coordinator, CoordinatorBroadcast> coord;
	private ClientManager<Worker, WorkerBroadcast> workers;
	private Logger log = Logger.getLogger(WorkerServer.class);

	public WorkerServer() throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;

		HashMap<String, String> data = new HashMap<>();
		data.put("type", "worker");

		JlimeFactory fact = new JlimeFactory(config, data);
		disp = fact.build();

		coord = disp.manage(new CoordinatorFactory(disp, "coordinator"),
				new PeerFilter() {

					@Override
					public boolean verify(Peer p) {
						return p.getData("type").equals("coordinator");
					}
				});

		workers = disp.manage(new WorkerFactory(disp, "worker"),
				new PeerFilter() {

					@Override
					public boolean verify(Peer p) {
						return p.getData("type").equals("worker");
					}
				});

	}

	public static void main(String[] args) throws Exception {
		new WorkerServer().start();
	}

	public void start() throws Exception {
		disp.start();

		disp.registerTarget("worker", new WorkerImpl(coord, workers));

		log.info("jLiME Worker Started");
	}

	public void stop() throws Exception {
		disp.stop();
	}
}
