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
import edu.jlime.pregel.graph.VertexFunction;
import java.lang.Integer;
import java.util.HashMap;
import java.util.List;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;
import java.util.UUID;
import edu.jlime.pregel.graph.Vertex;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;
import java.util.UUID;
import edu.jlime.pregel.graph.Vertex;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;
import java.util.UUID;
import java.util.UUID;
import java.lang.Boolean;
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

  public Map<Peer,PregelGraph>  execute(PregelGraph arg0, VertexFunction arg1, Integer arg2, HashMap<java.lang.String,edu.jlime.pregel.coordinator.Aggregator> arg3, List<edu.jlime.pregel.graph.Vertex> arg4) throws Exception {
    return disp.multiCall( dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

  public void setAggregatedValue(UUID arg0, Vertex arg1, String arg2, Double arg3) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "setAggregatedValue",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public Map<Peer,Double>  getAggregatedValue(UUID arg0, Vertex arg1, String arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAggregatedValue",new Object[] { arg0,arg1,arg2 });
  }

  public void finished(UUID arg0, UUID arg1, Boolean arg2) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "finished",new Object[] { arg0,arg1,arg2 });
  }

}