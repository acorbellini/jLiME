package edu.jlime.pregel.coordinator.rpc;

import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public interface CoordinatorBroadcast { 

  public Map<Peer,PregelExecution>  execute(VertexFunction arg0, PregelConfig arg1, Peer arg2) throws Exception; 

  public Map<Peer,Double>  getAggregatedValue(UUID arg0, Long arg1, String arg2) throws Exception; 

  public void setAggregatedValue(UUID arg0, Long arg1, String arg2, Double arg3) throws Exception; 

  public void finished(UUID arg0, UUID arg1, Boolean arg2) throws Exception; 

}