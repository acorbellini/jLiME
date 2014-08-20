package edu.jlime.pregel.worker;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.worker.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.lang.Exception;
import java.lang.Exception;
import edu.jlime.pregel.worker.VertexFunction;
import java.lang.Exception;

public class WorkerBroadcast implements Worker {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public WorkerBroadcast(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public void sendDataToVertex(int arg0, byte[] arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "sendDataToVertex",new Object[] { arg0,arg1 });
  }

  public void nextSuperstep(int arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "nextSuperstep",new Object[] { arg0 });
  }

  public void schedule(int arg0, VertexFunction arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "schedule",new Object[] { arg0,arg1 });
  }

}