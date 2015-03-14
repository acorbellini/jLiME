package edu.jlime.graphly.server;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.graphly.server.GraphlyCoordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.ConsistentHashing;
import java.lang.Exception;

public interface GraphlyCoordinatorBroadcast { 

  public Map<Peer,ConsistentHashing>  getHash() throws Exception; 

}