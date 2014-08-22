package edu.jlime.pregel.coordinator.rpc;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPCDispatcher;
public class CoordinatorFactory implements ClientFactory<Coordinator,CoordinatorBroadcast>{

  private RPCDispatcher rpc;

  private String target;

  public CoordinatorFactory(RPCDispatcher rpc, String target){
     this.rpc = rpc;
     this.target = target;
  }
  public CoordinatorBroadcast getBroadcast(List<Peer> to, Peer client){
    return new CoordinatorBroadcastImpl(rpc, to, client, target);
  }
  public Coordinator get(Peer to, Peer client){
    return new CoordinatorServerImpl(rpc, to, client, target);
  }
}