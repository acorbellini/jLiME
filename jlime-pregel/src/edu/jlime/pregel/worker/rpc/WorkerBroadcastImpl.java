package edu.jlime.pregel.worker.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;

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

  public void execute(UUID arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "execute",new Object[] { arg0 });
  }

  public Map<Peer,UUID>  getID() throws Exception {
    return disp.multiCall( dest, client, targetID, "getID",new Object[] {  });
  }

  public void sendMessages(List<edu.jlime.pregel.worker.PregelMessage> arg0, UUID arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "sendMessages",new Object[] { arg0,arg1 });
  }

  public void sendMessage(PregelMessage arg0, UUID arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "sendMessage",new Object[] { arg0,arg1 });
  }

  public void nextSuperstep(Integer arg0, UUID arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "nextSuperstep",new Object[] { arg0,arg1 });
  }

  public void createTask(UUID arg0, Peer arg1, VertexFunction arg2, PregelConfig arg3, Set<java.lang.Long> arg4) throws Exception {
    disp.multiCall( dest, client, targetID, "createTask",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

}