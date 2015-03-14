package edu.jlime.pregel.graph.rpc;


		
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.ClientFactory;

import java.util.List;

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
