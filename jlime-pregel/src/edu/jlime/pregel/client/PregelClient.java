package edu.jlime.pregel.client;

import java.util.HashMap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.PeerFilter;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class PregelClient {
	private RPCDispatcher rpc;
	private int minWorkers;

	public PregelClient(int numWorkers) throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		HashMap<String, String> data = new HashMap<>();
		data.put("type", "client");
		this.rpc = new JlimeFactory(config, data).build();
		this.rpc.start();
		this.minWorkers = numWorkers;
	}

	public PregelGraph execute(PregelGraph graph, VertexFunction minTree,
			Vertex... vList) throws Exception {

		rpc.manage(new CoordinatorFactory(rpc, "worker"), new PeerFilter() {
			@Override
			public boolean verify(Peer p) {
				return (p.getData("type").equals("worker"));
			}
		}).wait(minWorkers);

		return rpc
				.manage(new CoordinatorFactory(rpc, "coordinator"),
						new PeerFilter() {
							@Override
							public boolean verify(Peer p) {
								return (p.getData("type").equals("coordinator"));
							}
						}).waitFirst().execute(graph, minTree, vList, 10);
	}

	public void stop() throws Exception {
		rpc.stop();

	}
}
