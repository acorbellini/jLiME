package edu.jlime.pregel.coordinator.rpc;

import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public interface CoordinatorBroadcast { 

   public Map<Peer,PregelExecution>  execute(final VertexFunction arg0, final long[] arg1, final PregelConfig arg2, final Peer arg3) throws Exception; 

   public Map<Peer,Double>  getAggregatedValue(final int arg0, final Long arg1, final String arg2) throws Exception; 

   public void setAggregatedValue(final int arg0, final Long arg1, final String arg2, final Double arg3) throws Exception; 

   public void finished(final int arg0, final UUID arg1, final Boolean arg2) throws Exception; 

}