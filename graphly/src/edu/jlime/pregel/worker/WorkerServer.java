package edu.jlime.pregel.worker;

import java.util.HashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.rpc.NetworkConfiguration;

public class WorkerServer {
	public static final String WORKER_KEY = "pregel_worker";
	private RPCDispatcher disp;

	private ClientManager<Coordinator, CoordinatorBroadcast> coord;
	private ClientManager<Worker, WorkerBroadcast> workers;

	private Logger log = Logger.getLogger(WorkerServer.class);
	private WorkerImpl worker;

	public WorkerServer(RPCDispatcher disp) throws Exception {
		this.disp = disp;
		this.worker = new WorkerImpl(disp);
		disp.registerTarget(WORKER_KEY, worker, true);
	}

	public static WorkerServer main(String[] args) throws Exception {
		NetworkConfiguration config = new NetworkConfiguration();
		config.port = 6060;
		config.mcastport = 5050;

		HashMap<String, String> data = new HashMap<>();
		data.put("app", "graphly");
		data.put("type", WORKER_KEY);

		JLiMEFactory fact = new JLiMEFactory(config, data, new DataFilter(
				"app", "graphly", true));
		RPCDispatcher disp = fact.build();

		WorkerServer ws = new WorkerServer(disp);
		disp.start();
		return ws;
	}

	public void stop() {
		worker.stop();
	}
}
