package edu.jlime.pregel.client;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PageRank;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.pregel.coordinator.rpc.CoordinatorBroadcast;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class PregelClient {
	private RPCDispatcher rpc;
	private int minWorkers;
	ClientManager<Coordinator, CoordinatorBroadcast> workers;
	private Coordinator coordinator;

	public PregelClient(int numWorkers) throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		HashMap<String, String> data = new HashMap<>();
		data.put("type", "client");
		this.rpc = new JlimeFactory(config, data).build();
		this.rpc.start();
		this.minWorkers = numWorkers;

		workers = rpc.manage(new CoordinatorFactory(rpc, "worker"),
				new PeerFilter() {
					@Override
					public boolean verify(Peer p) {
						return (p.getData("type").equals("worker"));
					}
				});
		workers.wait(minWorkers);

		ClientManager<Coordinator, CoordinatorBroadcast> coordCli = rpc.manage(
				new CoordinatorFactory(rpc, "coordinator"), new PeerFilter() {
					@Override
					public boolean verify(Peer p) {
						return (p.getData("type").equals("coordinator"));
					}
				});
		this.coordinator = coordCli.waitFirst();
	}

	public void stop() throws Exception {
		rpc.stop();

	}

	public PregelGraph execute(PregelGraph graph, VertexFunction minTree,
			int maxSteps, Vertex... vList) throws Exception {
		return this.execute(graph, minTree, maxSteps, Arrays.asList(vList));

	}

	public PregelGraph execute(PregelGraph g, VertexFunction minTree,
			int maxSteps, List<Vertex> vList) throws Exception {
		return this.execute(g, minTree, maxSteps, new HashMap<>(), vList);
	}

	public PregelGraph execute(PregelGraph g, VertexFunction f, int maxSteps,
			HashMap<String, Aggregator> aggregators, List<Vertex> vList)
			throws Exception {
		return coordinator.execute(g, f, maxSteps, aggregators, vList);
	}
}
