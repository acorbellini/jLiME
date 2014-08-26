package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.Vertex;
import java.lang.Integer;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;
import java.util.UUID;
import java.util.UUID;
import java.lang.Boolean;
import java.lang.Exception;

public interface CoordinatorBroadcast { 

  public Map<Peer,PregelGraph>  execute(PregelGraph arg0, VertexFunction arg1, Vertex[] arg2, Integer arg3) throws Exception; 

  public void finished(UUID arg0, UUID arg1, Boolean arg2) throws Exception; 

}