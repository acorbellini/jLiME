package edu.jlime.pregel.coordinator;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.ClientFactory;
import java.util.List;
public class CoordinatorFactory implements ClientFactory<Coordinator>{

  private RPCDispatcher rpc;

  private String target;

  public CoordinatorFactory(RPCDispatcher rpc, String target){
     this.rpc = rpc;
     this.target = target;
  }
  public Coordinator getBroadcast(List<Peer> to, Peer client){
    return new CoordinatorBroadcast(rpc, to, client, target);
  }
  public Coordinator get(Peer to, Peer client){
    return new CoordinatorServerImpl(rpc, to, client, target);
  }
}