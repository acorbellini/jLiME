package edu.jlime.pregel.worker.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

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

  public Map<Peer,PregelGraph>  getResult(UUID arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getResult",new Object[] { arg0 });
  }

  public Map<Peer,UUID>  getID() throws Exception {
    return disp.multiCall( dest, client, targetID, "getID",new Object[] {  });
  }

  public Map<Peer,Boolean>  hasWork(UUID arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "hasWork",new Object[] { arg0 });
  }

  public void sendDataToVertex(Vertex arg0, Vertex arg1, byte[] arg2, UUID arg3) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "sendDataToVertex",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void createTask(PregelGraph arg0, VertexFunction arg1, UUID arg2, HashMap arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "createTask",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void nextSuperstep(int arg0, UUID arg1) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "nextSuperstep",new Object[] { arg0,arg1 });
  }

}