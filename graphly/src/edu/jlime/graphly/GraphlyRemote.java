package edu.jlime.graphly;

import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;
import edu.jlime.core.rpc.ClientManager;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.graphly.rpc.GraphlyStoreNodeIBroadcast;
import edu.jlime.graphly.rpc.GraphlyStoreNodeIFactory;

public class GraphlyRemote implements GraphlyStore {

	ClientManager<GraphlyStoreNodeI, GraphlyStoreNodeIBroadcast> mgr;

	public GraphlyRemote(RPCDispatcher rpc) {
		
		
		
		mgr = rpc.manage(new GraphlyStoreNodeIFactory(rpc, "graph-node"), rpc
				.getCluster().getLocalPeer());		
	}

	@Override
	public long[] getEdges(GetType type, long id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getVertexAttributes(long id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getEdgeAttributes(long id) {
		// TODO Auto-generated method stub
		return null;
	}

}
