package edu.jlime.graphly.server;

import edu.jlime.core.cluster.BroadcastException;
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

public class GraphlyCoordinatorBroadcastImpl implements GraphlyCoordinatorBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public GraphlyCoordinatorBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public Map<Peer,ConsistentHashing>  getHash() throws Exception {
    return disp.multiCall( dest, client, targetID, "getHash",new Object[] {  });
  }

}