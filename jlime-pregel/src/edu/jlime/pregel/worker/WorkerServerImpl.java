package edu.jlime.pregel.worker;

import edu.jlime.pregel.worker.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;

public class WorkerServerImpl implements Worker {

  RPCDispatcher disp;
  Peer local;
  Peer dest;
  Peer client;
  String targetID;

  public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest = dest;
    this.client = client;
    this.targetID = targetID;
  }

  public void sendDataToVertex(int arg0, byte[] arg1) throws Exception  {
    disp.callAsync(dest, client, targetID, "sendDataToVertex",new Object[] { arg0,arg1 });
  }

  public void nextSuperstep(int arg0) throws Exception  {
    disp.callAsync(dest, client, targetID, "nextSuperstep",new Object[] { arg0 });
  }

}