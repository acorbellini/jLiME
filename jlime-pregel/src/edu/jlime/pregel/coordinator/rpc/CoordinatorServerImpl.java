package edu.jlime.pregel.coordinator.rpc;

import java.util.HashMap;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.VertexFunction;

public class CoordinatorServerImpl extends RPCClient implements Coordinator {

  public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
  }

  public PregelGraph execute(PregelGraph arg0, HashMap arg1, VertexFunction arg2, int arg3)  throws Exception {
    return (PregelGraph) disp.callSync(dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void finished(UUID arg0, UUID arg1)  throws Exception {
    disp.callAsync(dest, client, targetID, "finished",new Object[] { arg0,arg1 });
  }

}