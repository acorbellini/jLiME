package edu.jlime.pregel.coordinator;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.coordinator.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;

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

  public void finished(int arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "finished",new Object[] { arg0 });
  }

}