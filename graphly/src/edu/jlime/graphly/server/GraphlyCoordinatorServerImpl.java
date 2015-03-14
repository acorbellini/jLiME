package edu.jlime.graphly.server;

import edu.jlime.graphly.server.GraphlyCoordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.ConsistentHashing;
import java.lang.Exception;

public class GraphlyCoordinatorServerImpl extends RPCClient implements GraphlyCoordinator, Transferible {

   transient RPCDispatcher local = null;
  public GraphlyCoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public ConsistentHashing getHash()  throws Exception {
if(local!=null) {
		return ((GraphlyCoordinator) local.getTarget(targetID) ).getHash();
}
    return (ConsistentHashing) disp.callSync(dest, client, targetID, "getHash",new Object[] {  });
  }

@Override
public void setRPC(RPCDispatcher rpc) {
this.disp=rpc;
}
}