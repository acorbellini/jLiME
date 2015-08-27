package edu.jlime.graphly.storenode.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.graphly.storenode.rpc.GraphlyStoreNodeI;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.lang.String;
import java.lang.String;
import java.lang.String;
import gnu.trove.list.array.TLongArrayList;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.util.Map;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.util.HashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import edu.jlime.graphly.rec.hits.DivideUpdateProperty;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.set.hash.TLongHashSet;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.list.array.TLongArrayList;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import gnu.trove.map.hash.TLongFloatHashMap;
import java.lang.Exception;
import java.lang.String;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import edu.jlime.graphly.util.Gather;
import java.lang.Object;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import java.lang.String;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.set.hash.TLongHashSet;
import edu.jlime.graphly.storenode.GraphlyCount;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.util.Set;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import gnu.trove.map.hash.TLongFloatHashMap;
import java.lang.Exception;

public interface GraphlyStoreNodeIBroadcast { 

   public void setProperty(final String arg0, final String arg1, final String arg2, final TLongArrayList arg3) throws Exception; 

   public void setProperty(final String arg0, final long arg1, final String arg2, final Object arg3) throws Exception; 

   public Map<Peer,Object>  getProperty(final String arg0, final long arg1, final String arg2) throws Exception; 

   public Map<Peer,Float>  getFloat(final String arg0, final long arg1, final String arg2, final float arg3) throws Exception; 

   public Map<Peer,Float>  getFloat(final String arg0, final long arg1, final String arg2) throws Exception; 

   public Map<Peer,Double>  getDouble(final String arg0, final long arg1, final String arg2) throws Exception; 

   public Map<Peer,Object>  getDefault(final String arg0, final String arg1) throws Exception; 

   public Map<Peer,Map>  getProperties(final String arg0, final long[] arg1, final String[] arg2) throws Exception; 

   public Map<Peer,TLongObjectHashMap>  getProperties(final String arg0, final String arg1, final int arg2, final TLongArrayList arg3) throws Exception; 

   public void setProperties(final String arg0, final String arg1, final TLongObjectHashMap<java.lang.Object> arg2) throws Exception; 

   public void setDouble(final String arg0, final long arg1, final String arg2, final double arg3) throws Exception; 

   public void setFloat(final String arg0, final long arg1, final String arg2, final float arg3) throws Exception; 

   public void setDefault(final String arg0, final String arg1, final Object arg2) throws Exception; 

   public void addInEdgePlaceholder(final String arg0, final long arg1, final long arg2, final String arg3) throws Exception; 

   public void commitFloatUpdates(final String arg0, final String[] arg1) throws Exception; 

   public void setTempProperties(final String arg0, final HashMap<java.lang.Long,java.util.Map<java.lang.String, java.lang.Object>> arg1) throws Exception; 

   public void updateFloatProperty(final String arg0, final String arg1, final DivideUpdateProperty arg2) throws Exception; 

   public void setEdgeProperty(final String arg0, final long arg1, final long arg2, final String arg3, final Object arg4, final String[] arg5) throws Exception; 

   public void addEdge(final String arg0, final long arg1, final long arg2, final String arg3, final Object[] arg4) throws Exception; 

   public void addRange(final int arg0) throws Exception; 

   public Map<Peer,String>  getLabel(final String arg0, final long arg1) throws Exception; 

   public void addEdges(final String arg0, final long arg1, final Dir arg2, final long[] arg3) throws Exception; 

   public Map<Peer,Integer>  getEdgeCount(final String arg0, final long arg1, final Dir arg2, final TLongHashSet arg3) throws Exception; 

   public Map<Peer,TLongArrayList>  getVertices(final String arg0, final long arg1, final int arg2, final boolean arg3) throws Exception; 

   public void setFloats(final String arg0, final String arg1, final TLongFloatHashMap arg2) throws Exception; 

   public Map<Peer,long[]>  getEdges(final String arg0, final Dir arg1, final int arg2, final long[] arg3) throws Exception; 

   public void addVertex(final String arg0, final long arg1, final String arg2) throws Exception; 

   public Map<Peer,Long>  getRandomEdge(final String arg0, final long arg1, final long[] arg2, final Dir arg3) throws Exception; 

   public Map<Peer,Integer>  getVertexCount(final String arg0) throws Exception; 

   public void setDefaultDouble(final String arg0, final String arg1, final double arg2) throws Exception; 

   public Map<Peer,Object>  gather(final String arg0, final Gather<?> arg1) throws Exception; 

   public Map<Peer,List>  getRanges() throws Exception; 

   public Map<Peer,GraphlyCount>  countEdges(final String arg0, final Dir arg1, final int arg2, final TLongFloatMap arg3, final TLongHashSet arg4) throws Exception; 

   public void commitUpdates(final String arg0, final String[] arg1) throws Exception; 

   public Map<Peer,Set>  getGraphs() throws Exception; 

   public Map<Peer,Double>  getDefaultDouble(final String arg0, final String arg1) throws Exception; 

   public void setDefaultFloat(final String arg0, final String arg1, final float arg2) throws Exception; 

   public Map<Peer,Object>  getEdgeProperty(final String arg0, final long arg1, final long arg2, final String arg3, final String[] arg4) throws Exception; 

   public void removeVertex(final String arg0, final long arg1) throws Exception; 

   public Map<Peer,Float>  getDefaultFloat(final String arg0, final String arg1) throws Exception; 

   public void setTempFloats(final String arg0, final String arg1, final boolean arg2, final TLongFloatHashMap arg3) throws Exception; 

}