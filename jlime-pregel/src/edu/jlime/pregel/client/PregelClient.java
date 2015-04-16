package edu.jlime.pregel.client;

import java.util.HashMap;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.WorkerFilter;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.pregel.worker.rpc.WorkerBroadcast;
import edu.jlime.pregel.worker.rpc.WorkerFactory;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JLiMEFactory;

public class PregelClient {

	private static final String PREGEL_CLIENT = "pregel_client";

	private RPCDispatcher rpc;
	private int minWorkers;
	private Coordinator coordinator;

	private ClientManager<Worker, WorkerBroadcast> workers;

	public PregelClient(RPCDispatcher rpc, int numWorkers) throws Exception {
		this.minWorkers = numWorkers;
		this.rpc = rpc;
		workers = rpc.manage(new WorkerFactory(rpc, WorkerServer.WORKER_KEY),
				new WorkerFilter(), this.rpc.getCluster().getLocalPeer());
		workers.waitForClient(minWorkers);

		ClientManager<Coordinator, CoordinatorBroadcast> coordCli = rpc.manage(
				new CoordinatorFactory(rpc, CoordinatorServer.COORDINATOR_KEY),
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

	public PregelExecution execute(VertexFunction f, long[] vList,
			PregelConfig conf) throws Exception {
		return coordinator.execute(f, vList, conf, this.rpc.getCluster()
				.getLocalPeer());
	}

	public RPCDispatcher getRPC() {
		return rpc;
	}

	public static WorkerFilter workerFilter() {
		return new WorkerFilter();
	}

	public static PregelClient build(int min) throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		HashMap<String, String> data = new HashMap<>();
		data.put("app", PREGEL_CLIENT);

		RPCDispatcher rpc = new JLiMEFactory(config, data, new DataFilter(
				"app", "pregel", true)).build();
		rpc.start();

		PregelClient cli = new PregelClient(rpc, min);
		return cli;
	}
}
