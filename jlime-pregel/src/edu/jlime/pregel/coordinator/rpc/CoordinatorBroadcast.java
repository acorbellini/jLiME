package edu.jlime.pregel.coordinator.rpc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.VertexFunction;

public interface CoordinatorBroadcast { 

  public Map<Peer,PregelGraph>  execute(PregelGraph arg0, HashMap arg1, VertexFunction arg2, int arg3) throws Exception; 

  public void finished(UUID arg0, UUID arg1) throws Exception; 

}