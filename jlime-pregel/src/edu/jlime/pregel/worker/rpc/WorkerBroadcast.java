package edu.jlime.pregel.worker.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.lang.Exception;
import edu.jlime.pregel.graph.Vertex;
import java.lang.Exception;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import java.lang.Exception;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;

public interface WorkerBroadcast { 

  public void nextSuperstep(int arg0) throws Exception; 

  public void sendDataToVertex(Vertex arg0, byte[] arg1) throws Exception; 

  public void schedule(Vertex arg0, VertexFunction arg1) throws Exception; 

  public void setGraph(PregelGraph arg0) throws Exception; 

}