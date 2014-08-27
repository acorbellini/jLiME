package edu.jlime.pregel.worker.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Exception;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.VertexFunction;
import java.util.UUID;
import java.util.HashSet;
import java.lang.Exception;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.worker.VertexData;
import java.util.UUID;
import java.lang.Exception;
import java.lang.Integer;
import java.util.UUID;
import java.lang.Exception;

public interface WorkerBroadcast { 

  public Map<Peer,PregelGraph>  getResult(UUID arg0) throws Exception; 

  public Map<Peer,UUID>  getID() throws Exception; 

  public void createTask(PregelGraph arg0, VertexFunction arg1, UUID arg2, HashSet<edu.jlime.pregel.graph.Vertex> arg3) throws Exception; 

  public void sendDataToVertex(Vertex arg0, Vertex arg1, VertexData arg2, UUID arg3) throws Exception; 

  public void nextSuperstep(Integer arg0, UUID arg1) throws Exception; 

}