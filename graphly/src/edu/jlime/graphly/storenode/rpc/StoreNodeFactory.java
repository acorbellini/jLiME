package edu.jlime.graphly.storenode.rpc;


		
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPC;

public class StoreNodeFactory implements ClientFactory<StoreNode,StoreNodeBroadcast>{
	private RPC rpc;
	private String target;

	public StoreNodeFactory(RPC rpc, String target){
		this.rpc = rpc;
		this.target = target;
	}

	public StoreNodeBroadcast getBroadcast(List<Peer> to, Peer client){
		return new StoreNodeBroadcastImpl(rpc, to, client, target);
	}

	public StoreNode get(Peer to, Peer client){
		return new StoreNodeServerImpl(rpc, to, client, target);
	}
}
