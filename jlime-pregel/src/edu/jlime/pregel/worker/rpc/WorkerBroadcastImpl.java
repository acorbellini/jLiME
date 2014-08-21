package edu.jlime.pregel.worker.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.lang.Exception;
import edu.jlime.pregel.graph.Vertex;
import java.lang.Exception;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import java.lang.Exception;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;

public class WorkerBroadcastImpl implements WorkerBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public WorkerBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public void nextSuperstep(int arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "nextSuperstep",new Object[] { arg0 });
  }

  public void sendDataToVertex(Vertex arg0, byte[] arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "sendDataToVertex",new Object[] { arg0,arg1 });
  }

  public void schedule(Vertex arg0, VertexFunction arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "schedule",new Object[] { arg0,arg1 });
  }

  public void setGraph(PregelGraph arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "setGraph",new Object[] { arg0 });
  }

}