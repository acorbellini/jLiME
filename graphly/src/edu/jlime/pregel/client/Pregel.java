package edu.jlime.pregel.client;

import java.util.HashMap;

import edu.jlime.core.cluster.DataFilter;
import edu.jlime.core.rpc.Client;
import edu.jlime.core.rpc.RPC;
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
import edu.jlime.rpc.JLiMEFactory;
import edu.jlime.rpc.NetworkConfiguration;

public class Pregel {

	private static final String PREGEL_CLIENT = "pregel_client";

	private RPC rpc;
	private int minWorkers;
	private Coordinator coordinator;

	private Client<Worker, WorkerBroadcast> workers;

	public Pregel(RPC rpc, int numWorkers) throws Exception {
		this.minWorkers = numWorkers;
		this.rpc = rpc;
		workers = rpc.manage(new WorkerFactory(rpc, WorkerServer.WORKER_KEY), new WorkerFilter(),
				this.rpc.getCluster().getLocalPeer());
		workers.waitForClient(minWorkers);

		Client<Coordinator, CoordinatorBroadcast> coordCli = rpc.manage(
				new CoordinatorFactory(rpc, CoordinatorServer.COORDINATOR_KEY), new CoordinatorFilter(),
				this.rpc.getCluster().getLocalPeer());
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

	public PregelExecution execute(VertexFunction f, long[] vList, PregelConfig conf) throws Exception {
		return coordinator.execute(f, vList, conf, this.rpc.getCluster().getLocalPeer());
	}

	public RPC getRPC() {
		return rpc;
	}

	public static WorkerFilter workerFilter() {
		return new WorkerFilter();
	}

	public static Pregel build(int min) throws Exception {
		NetworkConfiguration config = new NetworkConfiguration();
		config.port = 4040;
		config.mcastport = 5050;
		HashMap<String, String> data = new HashMap<>();
		data.put("app", PREGEL_CLIENT);

		RPC rpc = new JLiMEFactory(config, data, new DataFilter("app", "pregel", true)).build();
		rpc.start();

		Pregel cli = new Pregel(rpc, min);
		return cli;
	}
}
