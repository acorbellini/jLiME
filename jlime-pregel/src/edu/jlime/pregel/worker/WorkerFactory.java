package edu.jlime.pregel.worker;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.ClientFactory;
import java.util.List;
public class WorkerFactory implements ClientFactory<Worker>{

  private RPCDispatcher rpc;

  private String target;

  public WorkerFactory(RPCDispatcher rpc, String target){
     this.rpc = rpc;
     this.target = target;
  }
  public Worker getBroadcast(List<Peer> to, Peer client){
    return new WorkerBroadcast(rpc, to, client, target);
  }
  public Worker get(Peer to, Peer client){
    return new WorkerServerImpl(rpc, to, client, target);
  }
}