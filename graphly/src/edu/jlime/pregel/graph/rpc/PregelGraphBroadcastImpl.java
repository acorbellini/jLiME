package edu.jlime.pregel.graph.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.core.rpc.RPC;
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
import edu.jlime.pregel.worker.VertexList;
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
import java.lang.String;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import gnu.trove.set.hash.TLongHashSet;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import gnu.trove.set.hash.TLongHashSet;
import java.lang.Exception;
import edu.jlime.pregel.worker.VertexList;
import java.lang.Exception;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.set.hash.TLongHashSet;
import java.lang.Exception;
import java.util.Set;
import java.lang.Exception;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import gnu.trove.set.hash.TLongHashSet;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Iterable;
import java.lang.Exception;
import java.lang.Exception;

public class PregelGraphBroadcastImpl implements PregelGraphBroadcast {

  RPC disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public PregelGraphBroadcastImpl(RPC disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

   public Map<Peer,Object>  get(final long arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "get",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Float>  getFloat(final String arg0, final long arg1, final float arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getFloat",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Float>  getFloat(final long arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getFloat",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Double>  getDouble(final long arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDouble",new Object[] { arg0,arg1 });
  }

   public Map<Peer,String>  getName() throws Exception {
    return disp.multiCall( dest, client, targetID, "getName",new Object[] {  });
  }

   public Map<Peer,String>  print() throws Exception {
    return disp.multiCall( dest, client, targetID, "print",new Object[] {  });
  }

   public void flush(final VertexList arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "flush",new Object[] { arg0 });
  }

   public void setDouble(final long arg0, final String arg1, final double arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setDouble",new Object[] { arg0,arg1,arg2 });
  }

   public void setFloat(final long arg0, final String arg1, final float arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setFloat",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Object>  getDefaultValue(final String arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDefaultValue",new Object[] { arg0 });
  }

   public void setVal(final long arg0, final String arg1, final Object arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setVal",new Object[] { arg0,arg1,arg2 });
  }

   public void disable(final long arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "disable",new Object[] { arg0 });
  }

   public Map<Peer,Float>  getDefaultFloat(final String arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDefaultFloat",new Object[] { arg0 });
  }

   public Map<Peer,Integer>  getNeighbourhoodSize(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getNeighbourhoodSize",new Object[] { arg0 });
  }

   public void putLink(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "putLink",new Object[] { arg0,arg1 });
  }

   public void putIncoming(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "putIncoming",new Object[] { arg0,arg1 });
  }

   public void removeOutgoing(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "removeOutgoing",new Object[] { arg0,arg1 });
  }

   public Map<Peer,TLongHashSet>  getNeighbours(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getNeighbours",new Object[] { arg0 });
  }

   public Map<Peer,Boolean>  createVertex(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "createVertex",new Object[] { arg0 });
  }

   public Map<Peer,Integer>  getAdyacencySize(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAdyacencySize",new Object[] { arg0 });
  }

   public Map<Peer,Integer>  getOutgoingSize(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getOutgoingSize",new Object[] { arg0 });
  }

   public void setDefaultValue(final String arg0, final Object arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "setDefaultValue",new Object[] { arg0,arg1 });
  }

   public Map<Peer,TLongHashSet>  getOutgoing(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getOutgoing",new Object[] { arg0 });
  }

   public void preload(final VertexList arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "preload",new Object[] { arg0 });
  }

   public void disableOutgoing(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "disableOutgoing",new Object[] { arg0,arg1 });
  }

   public Map<Peer,TLongHashSet>  getAdjacents(final long arg0, final Dir arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAdjacents",new Object[] { arg0,arg1 });
  }

   public void createVertices(final Set<java.lang.Long> arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "createVertices",new Object[] { arg0 });
  }

   public void enableAll() throws Exception {
    disp.multiCall( dest, client, targetID, "enableAll",new Object[] {  });
  }

   public void putOutgoing(final List<long[]> arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "putOutgoing",new Object[] { arg0 });
  }

   public void putOutgoing(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "putOutgoing",new Object[] { arg0,arg1 });
  }

   public void disableLink(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "disableLink",new Object[] { arg0,arg1 });
  }

   public Map<Peer,TLongHashSet>  getIncoming(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getIncoming",new Object[] { arg0 });
  }

   public void disableIncoming(final long arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "disableIncoming",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Boolean>  isLocal(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "isLocal",new Object[] { arg0 });
  }

   public Map<Peer,Iterable>  vertices() throws Exception {
    return disp.multiCall( dest, client, targetID, "vertices",new Object[] {  });
  }

   public Map<Peer,Integer>  vertexSize() throws Exception {
    return disp.multiCall( dest, client, targetID, "vertexSize",new Object[] {  });
  }

}