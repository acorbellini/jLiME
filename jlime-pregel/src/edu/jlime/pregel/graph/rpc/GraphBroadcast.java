package edu.jlime.pregel.graph.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.lang.Long;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Long;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.util.Set;
import java.util.List;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Long;
import java.util.Collection;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.util.Collection;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.util.Collection;
import java.lang.Exception;

public interface GraphBroadcast { 

  public Map<Peer,Object>  get(Long arg0, String arg1) throws Exception; 

  public Map<Peer,String>  getName() throws Exception; 

  public Map<Peer,String>  print() throws Exception; 

  public Map<Peer,Object>  getDefaultValue(String arg0) throws Exception; 

  public void setVal(Long arg0, String arg1, Object arg2) throws Exception; 

  public void disable(Long arg0) throws Exception; 

  public void createVertices(Set<java.lang.Long> arg0) throws Exception; 

  public void putOutgoing(List<java.lang.Long[]> arg0) throws Exception; 

  public void putOutgoing(Long arg0, Long arg1) throws Exception; 

  public Map<Peer,Integer>  getAdyacencySize(Long arg0) throws Exception; 

  public void putLink(Long arg0, Long arg1) throws Exception; 

  public Map<Peer,Integer>  getOutgoingSize(Long arg0) throws Exception; 

  public void disableIncoming(Long arg0, Long arg1) throws Exception; 

  public void disableLink(Long arg0, Long arg1) throws Exception; 

  public void disableOutgoing(Long arg0, Long arg1) throws Exception; 

  public Map<Peer,Integer>  vertexSize() throws Exception; 

  public void setDefaultValue(String arg0, Object arg1) throws Exception; 

  public Map<Peer,Collection>  getOutgoing(Long arg0) throws Exception; 

  public Map<Peer,Boolean>  createVertex(Long arg0) throws Exception; 

  public Map<Peer,Collection>  getIncoming(Long arg0) throws Exception; 

  public void putIncoming(Long arg0, Long arg1) throws Exception; 

  public void enableAll() throws Exception; 

  public void removeOutgoing(Long arg0, Long arg1) throws Exception; 

  public Map<Peer,Collection>  vertices() throws Exception; 

}