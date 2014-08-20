package edu.jlime.pregel.worker;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.worker.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.cluster.Peer;

import java.util.List;
import java.util.ArrayList;

public interface WorkerBroadcast { 

  public void sendDataToVertex(int arg0, byte[] arg1) throws Exception; 

  public void nextSuperstep(int arg0) throws Exception; 

}