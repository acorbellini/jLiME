package edu.jlime.graphly.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;

import java.lang.Long;

public interface GraphlyStoreNodeIBroadcast { 

  public Map<Peer,long[]>  getEdges(final GetType arg0, final Long arg1) throws Exception; 

}