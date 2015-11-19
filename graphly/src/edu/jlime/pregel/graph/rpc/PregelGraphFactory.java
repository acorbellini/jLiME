package edu.jlime.pregel.graph.rpc;


		
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.ClientFactory;

import java.util.List;

public class PregelGraphFactory implements ClientFactory<PregelGraph,PregelGraphBroadcast>{
	private RPC rpc;
	private String target;

	public PregelGraphFactory(RPC rpc, String target){
		this.rpc = rpc;
		this.target = target;
	}

	public PregelGraphBroadcast getBroadcast(List<Peer> to, Peer client){
		return new PregelGraphBroadcastImpl(rpc, to, client, target);
	}

	public PregelGraph get(Peer to, Peer client){
		return new PregelGraphServerImpl(rpc, to, client, target);
	}
}
