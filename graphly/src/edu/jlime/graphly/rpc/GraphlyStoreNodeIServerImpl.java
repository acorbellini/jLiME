package edu.jlime.graphly.rpc;

import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.GetType;

import java.lang.Long;

public class GraphlyStoreNodeIServerImpl extends RPCClient implements GraphlyStoreNodeI {

    RPCDispatcher local = null;
  public GraphlyStoreNodeIServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public long[] getEdges(final GetType arg0, final Long arg1) throws Exception  {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getEdges(arg0,arg1);
}
    return (long[]) disp.callSync(dest, client, targetID, "getEdges",new Object[] { arg0,arg1 });
  }

}