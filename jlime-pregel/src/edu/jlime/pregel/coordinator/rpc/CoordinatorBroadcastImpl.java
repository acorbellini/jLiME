package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.pregel.graph.PregelGraph;
import java.util.HashMap;
import edu.jlime.pregel.graph.VertexFunction;
import java.lang.Integer;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;
import java.util.UUID;
import java.util.UUID;
import java.lang.Exception;

public class CoordinatorBroadcastImpl implements CoordinatorBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public CoordinatorBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public Map<Peer,PregelGraph>  execute(PregelGraph arg0, HashMap<edu.jlime.pregel.graph.Vertex,edu.jlime.pregel.worker.VertexData> arg1, VertexFunction arg2, Integer arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void finished(UUID arg0, UUID arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "finished",new Object[] { arg0,arg1 });
  }

}