package edu.jlime.graphly.server;


		
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPCDispatcher;

public class GraphlyCoordinatorFactory implements ClientFactory<GraphlyCoordinator,GraphlyCoordinatorBroadcast>{
	private RPCDispatcher rpc;
	private String target;

	public GraphlyCoordinatorFactory(RPCDispatcher rpc, String target){
		this.rpc = rpc;
		this.target = target;
	}

	public GraphlyCoordinatorBroadcast getBroadcast(List<Peer> to, Peer client){
		return new GraphlyCoordinatorBroadcastImpl(rpc, to, client, target);
	}

	public GraphlyCoordinator get(Peer to, Peer client){
		return new GraphlyCoordinatorServerImpl(rpc, to, client, target);
	}
}