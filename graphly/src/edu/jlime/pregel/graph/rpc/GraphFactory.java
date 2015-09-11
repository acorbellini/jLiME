package edu.jlime.pregel.graph.rpc;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPC;

public class GraphFactory implements ClientFactory<PregelGraph, PregelGraphBroadcast> {
	private RPC rpc;
	private String target;

	public GraphFactory(RPC rpc, String target) {
		this.rpc = rpc;
		this.target = target;
	}

	public PregelGraphBroadcast getBroadcast(List<Peer> to, Peer client) {
		return new GraphBroadcastImpl(rpc, to, client, target);
	}

	public PregelGraph get(Peer to, Peer client) {
		return new GraphServerImpl(rpc, to, client, target);
	}
}
