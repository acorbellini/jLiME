package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.PregelExecution;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Long;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Long;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;
import java.util.UUID;
import java.util.UUID;
import java.lang.Boolean;
import java.lang.Exception;

public interface CoordinatorBroadcast { 

  public Map<Peer,PregelExecution>  execute(VertexFunction arg0, PregelConfig arg1, Peer arg2) throws Exception; 

  public Map<Peer,Double>  getAggregatedValue(UUID arg0, Long arg1, String arg2) throws Exception; 

  public void setAggregatedValue(UUID arg0, Long arg1, String arg2, Double arg3) throws Exception; 

  public void finished(UUID arg0, UUID arg1, Boolean arg2) throws Exception; 

}