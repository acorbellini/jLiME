package edu.jlime.pregel.client;

import java.util.HashMap;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.coordinator.rpc.CoordinatorFactory;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.JlimeFactory;

public class PregelClient {
	private RPCDispatcher rpc;

	public PregelClient() throws Exception {
		Configuration config = new Configuration();
		config.port = 4040;
		config.mcastport = 5050;
		HashMap<String, String> data = new HashMap<>();
		data.put("type", "client");
		this.rpc = new JlimeFactory(config, data).build();
		this.rpc.start();
	}

	public PregelGraph execute(PregelGraph graph, HashMap<Vertex, byte[]> data,
			VertexFunction func) throws Exception {
		return rpc.manage(new CoordinatorFactory(rpc, "coordinator")).first()
				.execute(graph, data, func, 10);
	}
}
