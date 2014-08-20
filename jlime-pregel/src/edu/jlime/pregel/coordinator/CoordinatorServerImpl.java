package edu.jlime.pregel.coordinator;

import edu.jlime.pregel.coordinator.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.List;
import edu.jlime.pregel.worker.VertexFunction;
import java.lang.Exception;
import java.lang.Exception;

public class CoordinatorServerImpl extends RPCClient implements Coordinator {

  public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
  }

  public void execute(List arg0, VertexFunction arg1)  throws Exception {
    disp.callAsync(dest, client, targetID, "execute",new Object[] { arg0,arg1 });
  }

  public void finished(int arg0)  throws Exception {
    disp.callAsync(dest, client, targetID, "finished",new Object[] { arg0 });
  }

}