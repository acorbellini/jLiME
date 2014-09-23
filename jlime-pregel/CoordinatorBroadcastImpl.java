package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.PregelExecution;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Long;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Long;
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

  public Map<Peer,PregelExecution>  execute(VertexFunction arg0, PregelConfig arg1, Peer arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2 });
  }

  public Map<Peer,Double>  getAggregatedValue(UUID arg0, Long arg1, String arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAggregatedValue",new Object[] { arg0,arg1,arg2 });
  }

  public void setAggregatedValue(UUID arg0, Long arg1, String arg2, Double arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "setAggregatedValue",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void finished(UUID arg0, UUID arg1, Boolean arg2) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "finished",new Object[] { arg0,arg1,arg2 });
  }

}