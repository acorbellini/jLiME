package edu.jlime.graphly.traversal;

import edu.jlime.core.rpc.RPC;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.jd.Dispatcher;
import edu.jlime.pregel.client.GraphConnectionFactory;
import edu.jlime.pregel.graph.rpc.PregelGraph;

public final class GraphlyGraphConnectionFactory implements GraphConnectionFactory {
	private String name;

	public GraphlyGraphConnectionFactory(String name) {
		this.name = name;
	}

	@Override
	public PregelGraph getGraph(RPC rpc) throws Exception {
		Dispatcher jobDispatcher = (Dispatcher) rpc.getTarget("JD");
		Graphly g = (Graphly) jobDispatcher.getGlobal("graphly");
		return new GraphlyPregelAdapter(g.getGraph(name));
	}
}