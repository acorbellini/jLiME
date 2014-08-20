package edu.jlime.pregel.coordinator;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.coordinator.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;

public interface CoordinatorBroadcast { 

  public void finished(int arg0) throws Exception; 

}