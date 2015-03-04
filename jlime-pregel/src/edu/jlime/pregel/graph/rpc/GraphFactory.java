package edu.jlime.pregel.graph.rpc;


		
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPCDispatcher;

public class GraphFactory implements ClientFactory<Graph,GraphBroadcast>{
	private RPCDispatcher rpc;
	private String target;

	public GraphFactory(RPCDispatcher rpc, String target){
		this.rpc = rpc;
		this.target = target;
	}

	public GraphBroadcast getBroadcast(List<Peer> to, Peer client){
		return new GraphBroadcastImpl(rpc, to, client, target);
	}

	public Graph get(Peer to, Peer client){
		return new GraphServerImpl(rpc, to, client, target);
	}
}
