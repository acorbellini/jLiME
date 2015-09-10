package edu.jlime.graphly.storenode.rpc;


		
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.ClientFactory;

import java.util.List;

public class GraphlyStoreNodeIFactory implements ClientFactory<GraphlyStoreNodeI,GraphlyStoreNodeIBroadcast>{
	private RPCDispatcher rpc;
	private String target;

	public GraphlyStoreNodeIFactory(RPCDispatcher rpc, String target){
		this.rpc = rpc;
		this.target = target;
	}

	public GraphlyStoreNodeIBroadcast getBroadcast(List<Peer> to, Peer client){
		return new GraphlyStoreNodeIBroadcastImpl(rpc, to, client, target);
	}

	public GraphlyStoreNodeI get(Peer to, Peer client){
		return new GraphlyStoreNodeIServerImpl(rpc, to, client, target);
	}
}
