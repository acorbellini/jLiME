package edu.jlime.pregel.worker;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import java.util.List;
public class WorkerFactory {

  private RPCDispatcher rpc;

  private String target;

  public WorkerFactory(RPCDispatcher rpc, String target){
     this.rpc = rpc;
     this.target = target;
  }
  public WorkerBroadcast getBroadcast(List<Peer> to, Peer client){
    return new WorkerBroadcastImpl(rpc, to, client, target);
  }
  public Worker get(Peer to, Peer client){
    return new WorkerServerImpl(rpc, to, client, target);
  }
}