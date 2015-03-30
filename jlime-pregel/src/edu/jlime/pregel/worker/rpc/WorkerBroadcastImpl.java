package edu.jlime.pregel.worker.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.util.UUID;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Exception;
import java.util.UUID;
import edu.jlime.pregel.client.SplitFunction;
import java.lang.Exception;
import edu.jlime.pregel.worker.PregelMessage;
import java.util.UUID;
import java.lang.Exception;
import java.util.List;
import java.util.UUID;
import java.lang.Exception;
import java.util.UUID;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.client.PregelConfig;
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

  public void execute(final UUID arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "execute",new Object[] { arg0 });
  }

  public Map<Peer,UUID>  getID() throws Exception {
    return disp.multiCall( dest, client, targetID, "getID",new Object[] {  });
  }

  public void nextSuperstep(final int arg0, final UUID arg1, final SplitFunction arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "nextSuperstep",new Object[] { arg0,arg1,arg2 });
  }

  public void sendMessage(final PregelMessage arg0, final UUID arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "sendMessage",new Object[] { arg0,arg1 });
  }

  public void sendMessages(final List<edu.jlime.pregel.worker.PregelMessage> arg0, final UUID arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "sendMessages",new Object[] { arg0,arg1 });
  }

  public void createTask(final UUID arg0, final Peer arg1, final VertexFunction arg2, final PregelConfig arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "createTask",new Object[] { arg0,arg1,arg2,arg3 });
  }

}