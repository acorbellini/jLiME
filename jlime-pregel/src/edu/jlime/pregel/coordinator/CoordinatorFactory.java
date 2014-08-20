package edu.jlime.pregel.coordinator;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import java.util.List;
public class CoordinatorFactory {

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