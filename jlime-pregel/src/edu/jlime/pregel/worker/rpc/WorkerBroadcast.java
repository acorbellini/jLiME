package edu.jlime.pregel.worker.rpc;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;

public interface WorkerBroadcast { 

  public Map<Peer,PregelGraph>  getResult(UUID arg0) throws Exception; 

  public Map<Peer,UUID>  getID() throws Exception; 

  public Map<Peer,Boolean>  hasWork(UUID arg0) throws Exception; 

  public void sendDataToVertex(Vertex arg0, Vertex arg1, byte[] arg2, UUID arg3) throws Exception; 

  public void createTask(PregelGraph arg0, VertexFunction arg1, UUID arg2, HashMap arg3) throws Exception; 

  public void nextSuperstep(int arg0, UUID arg1) throws Exception; 

}