package edu.jlime.graphly.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;

import java.lang.Long;

public class GraphlyStoreNodeIBroadcastImpl implements GraphlyStoreNodeIBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public GraphlyStoreNodeIBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public Map<Peer,long[]>  getEdges(final GetType arg0, final Long arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdges",new Object[] { arg0,arg1 });
  }

}