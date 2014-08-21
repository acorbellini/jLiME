package edu.jlime.pregel.worker.rpc;

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

public class WorkerServerImpl extends RPCClient implements Worker {

  public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
  }

  public void nextSuperstep(int arg0)  throws Exception {
    disp.callAsync(dest, client, targetID, "nextSuperstep",new Object[] { arg0 });
  }

  public void sendDataToVertex(Vertex arg0, byte[] arg1)  throws Exception {
    disp.callAsync(dest, client, targetID, "sendDataToVertex",new Object[] { arg0,arg1 });
  }

  public void schedule(Vertex arg0, VertexFunction arg1)  throws Exception {
    disp.callAsync(dest, client, targetID, "schedule",new Object[] { arg0,arg1 });
  }

  public void setGraph(PregelGraph arg0)  throws Exception {
    disp.callSync(dest, client, targetID, "setGraph",new Object[] { arg0 });
  }

}