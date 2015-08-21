package edu.jlime.graphly.traversal;

import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.client.GraphlyClient;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.pregel.client.GraphConnectionFactory;
import edu.jlime.pregel.graph.rpc.Graph;

public final class GraphlyGraphConnectionFactory implements
		GraphConnectionFactory {
	private String name;

	public GraphlyGraphConnectionFactory(String name) {
		this.name = name;
	}

	@Override
	public Graph getGraph(RPCDispatcher rpc) throws Exception {
		JobDispatcher jobDispatcher = (JobDispatcher) rpc.getTarget("JD");
		GraphlyClient g = (GraphlyClient) jobDispatcher.getGlobal("graphly");
		return new GraphlyPregelAdapter(g.getGraph(name));
	}
}