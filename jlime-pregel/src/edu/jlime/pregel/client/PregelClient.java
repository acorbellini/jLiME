package edu.jlime.pregel.client;

import java.util.HashMap;

import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.WorkerFilter;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class PregelClient {

	private RPCDispatcher rpc;
	private int minWorkers;
	private Coordinator coordinator;
	private ClientManager<Worker, WorkerBroadcast> workers;

	public PregelClient(int numWorkers) throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		HashMap<String, String> data = new HashMap<>();
		data.put("type", "client");
		this.rpc = new JlimeFactory(config, data).build();
		this.rpc.start();
		this.minWorkers = numWorkers;

		workers = rpc.manage(new WorkerFactory(rpc, "worker"),
				new WorkerFilter(), this.rpc.getCluster().getLocalPeer());
		workers.waitForClient(minWorkers);

		ClientManager<Coordinator, CoordinatorBroadcast> coordCli = rpc.manage(
				new CoordinatorFactory(rpc, "coordinator"),
				new CoordinatorFilter(), this.rpc.getCluster().getLocalPeer());
		this.coordinator = coordCli.waitFirst();
	}

	public void stop() throws Exception {
		rpc.stop();

	}

	// public PregelGraph execute(PregelGraph graph, VertexFunction minTree,
	// int maxSteps, Long... vList) throws Exception {
	// return this.execute(graph, minTree, maxSteps, Arrays.asList(vList));
	//
	// }

	public PregelExecution execute(VertexFunction f, PregelConfig conf)
			throws Exception {
		return coordinator.execute(f, conf, this.rpc.getCluster()
				.getLocalPeer());
	}

	public RPCDispatcher getRPC() {
		return rpc;
	}

	public static WorkerFilter workerFilter() {
		return new WorkerFilter();
	}
}
