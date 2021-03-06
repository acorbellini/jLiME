package edu.jlime.graphly.storenode.rpc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.List;
import java.util.Map;
import java.util.Map;
import java.util.Map;
import java.util.Map;
import java.util.Set;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import edu.jlime.graphly.storenode.Count;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.util.Gather;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.set.hash.TLongHashSet;

public class StoreNodeBroadcastImpl implements StoreNodeBroadcast {

  RPC disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public StoreNodeBroadcastImpl(RPC disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

   public void setProperty(final String arg0, final String arg1, final String arg2, final TLongArrayList arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "setProperty",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public void setProperty(final String arg0, final long arg1, final String arg2, final Object arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "setProperty",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,Object>  getProperty(final String arg0, final long arg1, final String arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getProperty",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Float>  getFloat(final String arg0, final long arg1, final String arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getFloat",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Float>  getFloat(final String arg0, final long arg1, final String arg2, final float arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getFloat",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,Double>  getDouble(final String arg0, final long arg1, final String arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDouble",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Object>  getDefault(final String arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDefault",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Map>  getProperties(final String arg0, final long[] arg1, final String[] arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getProperties",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,TLongObjectMap>  getProperties(final String arg0, final String arg1, final int arg2, final TLongArrayList arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getProperties",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public void setProperties(final String arg0, final String arg1, final TLongObjectMap<java.lang.Object> arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setProperties",new Object[] { arg0,arg1,arg2 });
  }

   public void setDouble(final String arg0, final long arg1, final String arg2, final double arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "setDouble",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public void setFloat(final String arg0, final long arg1, final String arg2, final float arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "setFloat",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public void setDefault(final String arg0, final String arg1, final Object arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setDefault",new Object[] { arg0,arg1,arg2 });
  }

   public void updateFloatProperty(final String arg0, final String arg1, final DivideUpdateProperty arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "updateFloatProperty",new Object[] { arg0,arg1,arg2 });
  }

   public void addInEdgePlaceholder(final String arg0, final long arg1, final long arg2, final String arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "addInEdgePlaceholder",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public void commitFloatUpdates(final String arg0, final String[] arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "commitFloatUpdates",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Map>  getAllFloatProperties(final String arg0, final long[] arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAllFloatProperties",new Object[] { arg0,arg1 });
  }

   public void setTempProperties(final String arg0, final HashMap<java.lang.Long,java.util.Map<java.lang.String, java.lang.Object>> arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "setTempProperties",new Object[] { arg0,arg1 });
  }

   public Map<Peer,String>  getLabel(final String arg0, final long arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getLabel",new Object[] { arg0,arg1 });
  }

   public void commitUpdates(final String arg0, final String[] arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "commitUpdates",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Object>  gather(final String arg0, final Gather<?> arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "gather",new Object[] { arg0,arg1 });
  }

   public void setFloats(final String arg0, final String arg1, final TLongFloatMap arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setFloats",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,List>  getRanges() throws Exception {
    return disp.multiCall( dest, client, targetID, "getRanges",new Object[] {  });
  }

   public void setEdgeProperty(final String arg0, final long arg1, final long arg2, final String arg3, final Object arg4, final String[] arg5) throws Exception {
    disp.multiCall( dest, client, targetID, "setEdgeProperty",new Object[] { arg0,arg1,arg2,arg3,arg4,arg5 });
  }

   public void addRange(final int arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "addRange",new Object[] { arg0 });
  }

   public void addEdge(final String arg0, final long arg1, final long arg2, final String arg3, final Object[] arg4) throws Exception {
    disp.multiCall( dest, client, targetID, "addEdge",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

   public void removeVertex(final String arg0, final long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "removeVertex",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Integer>  getEdgeCount(final String arg0, final long arg1, final Dir arg2, final TLongHashSet arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdgeCount",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,Integer>  getVertexCount(final String arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getVertexCount",new Object[] { arg0 });
  }

   public Map<Peer,long[]>  getEdges(final String arg0, final Dir arg1, final int arg2, final long[] arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdges",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,Long>  getRandomEdge(final String arg0, final long arg1, final long[] arg2, final Dir arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getRandomEdge",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,TLongArrayList>  getVertices(final String arg0, final long arg1, final int arg2, final boolean arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getVertices",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,Set>  getGraphs() throws Exception {
    return disp.multiCall( dest, client, targetID, "getGraphs",new Object[] {  });
  }

   public void setDefaultFloat(final String arg0, final String arg1, final float arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setDefaultFloat",new Object[] { arg0,arg1,arg2 });
  }

   public void addEdges(final String arg0, final long arg1, final Dir arg2, final long[] arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "addEdges",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public Map<Peer,Object>  getEdgeProperty(final String arg0, final long arg1, final long arg2, final String arg3, final String[] arg4) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdgeProperty",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

   public Map<Peer,Float>  getDefaultFloat(final String arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDefaultFloat",new Object[] { arg0,arg1 });
  }

   public void addVertex(final String arg0, final long arg1, final String arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "addVertex",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Count>  countEdges(final String arg0, final Dir arg1, final int arg2, final long[] arg3, final float[] arg4, final long[] arg5) throws Exception {
    return disp.multiCall( dest, client, targetID, "countEdges",new Object[] { arg0,arg1,arg2,arg3,arg4,arg5 });
  }

   public void setDefaultDouble(final String arg0, final String arg1, final double arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setDefaultDouble",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Double>  getDefaultDouble(final String arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDefaultDouble",new Object[] { arg0,arg1 });
  }

   public void setTempFloats(final String arg0, final String arg1, final boolean arg2, final long[] arg3, final float[] arg4) throws Exception {
    disp.multiCall( dest, client, targetID, "setTempFloats",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

   public void createSubgraph(final String arg0, final String arg1, final long[] arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "createSubgraph",new Object[] { arg0,arg1,arg2 });
  }

   public Map<Peer,Boolean>  containsVertex(final String arg0, final long arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "containsVertex",new Object[] { arg0,arg1 });
  }

   public Map<Peer,Map>  getAllProperties(final String arg0, final long[] arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAllProperties",new Object[] { arg0,arg1 });
  }

   public Map<Peer,AdjacencyData>  getAllEdges(final String arg0, final TLongArrayList arg1, final Dir arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAllEdges",new Object[] { arg0,arg1,arg2 });
  }

}