package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Exception;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;

public interface CoordinatorBroadcast { 

  public Map<Peer,PregelGraph>  execute(List arg0, VertexFunction arg1) throws Exception; 

  public void finished(UUID arg0) throws Exception; 

  public void setGraph(PregelGraph arg0) throws Exception; 

}