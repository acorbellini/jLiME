package edu.jlime.pregel.coordinator;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.worker.VertexFunction;

public class CoordinatorBroadcast implements Coordinator {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public CoordinatorBroadcast(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public void execute(List arg0, VertexFunction arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "execute",new Object[] { arg0,arg1 });
  }

  public void finished(int arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "finished",new Object[] { arg0 });
  }

}