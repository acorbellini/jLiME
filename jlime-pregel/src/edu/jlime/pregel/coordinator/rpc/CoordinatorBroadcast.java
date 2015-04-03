package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
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

   public Map<Peer,PregelExecution>  execute(final VertexFunction arg0, final PregelConfig arg1, final Peer arg2) throws Exception; 

   public void setAggregatedValue(final UUID arg0, final Long arg1, final String arg2, final Double arg3) throws Exception; 

   public Map<Peer,Double>  getAggregatedValue(final UUID arg0, final Long arg1, final String arg2) throws Exception; 

   public void finished(final UUID arg0, final UUID arg1, final Boolean arg2) throws Exception; 

}