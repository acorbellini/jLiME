package edu.jlime.pregel.graph.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.util.Set;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import gnu.trove.list.array.TLongArrayList;
import java.lang.Exception;
import java.lang.Iterable;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import gnu.trove.list.array.TLongArrayList;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;

public interface GraphBroadcast { 

   public Map<Peer,Object>  get(final long arg0, final String arg1) throws Exception; 

   public Map<Peer,Float>  getFloat(final long arg0, final String arg1) throws Exception; 

   public Map<Peer,Double>  getDouble(final long arg0, final String arg1) throws Exception; 

   public Map<Peer,String>  getName() throws Exception; 

   public Map<Peer,String>  print() throws Exception; 

   public void setDouble(final long arg0, final String arg1, final double arg2) throws Exception; 

   public void setFloat(final long arg0, final String arg1, final float arg2) throws Exception; 

   public Map<Peer,Object>  getDefaultValue(final String arg0) throws Exception; 

   public void setVal(final long arg0, final String arg1, final Object arg2) throws Exception; 

   public void disable(final long arg0) throws Exception; 

   public Map<Peer,Integer>  getAdyacencySize(final long arg0) throws Exception; 

   public void putOutgoing(final long arg0, final long arg1) throws Exception; 

   public void putOutgoing(final List<long[]> arg0) throws Exception; 

   public Map<Peer,Integer>  getOutgoingSize(final long arg0) throws Exception; 

   public void disableLink(final long arg0, final long arg1) throws Exception; 

   public void disableIncoming(final long arg0, final long arg1) throws Exception; 

   public void createVertices(final Set<java.lang.Long> arg0) throws Exception; 

   public void putLink(final long arg0, final long arg1) throws Exception; 

   public void setDefaultValue(final String arg0, final Object arg1) throws Exception; 

   public void enableAll() throws Exception; 

   public void disableOutgoing(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,Boolean>  createVertex(final long arg0) throws Exception; 

   public Map<Peer,TLongArrayList>  getOutgoing(final long arg0) throws Exception; 

   public Map<Peer,Iterable>  vertices() throws Exception; 

   public void removeOutgoing(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,Float>  getDefaultFloat(final String arg0) throws Exception; 

   public Map<Peer,TLongArrayList>  getIncoming(final long arg0) throws Exception; 

   public void putIncoming(final long arg0, final long arg1) throws Exception; 

   public Map<Peer,Integer>  vertexSize() throws Exception; 

}