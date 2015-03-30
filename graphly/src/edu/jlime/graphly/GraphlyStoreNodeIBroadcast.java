package edu.jlime.graphly;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.graphly.GraphlyStoreNodeI;
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
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import java.util.Map;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.util.HashMap;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.GraphlyCount;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.Exception;
import gnu.trove.list.array.TLongArrayList;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;

public interface GraphlyStoreNodeIBroadcast { 

  public void setProperty(final long arg0, final String arg1, final Object arg2) throws Exception; 

  public Map<Peer,Object>  getProperty(final long arg0, final String arg1) throws Exception; 

  public Map<Peer,Object>  getDefault(final String arg0) throws Exception; 

  public Map<Peer,TLongObjectHashMap>  getProperties(final String arg0, final int arg1, final TLongArrayList arg2) throws Exception; 

  public Map<Peer,Map>  getProperties(final long[] arg0, final String[] arg1) throws Exception; 

  public void setProperties(final String arg0, final TLongObjectHashMap<java.lang.Object> arg1) throws Exception; 

  public void setDefault(final String arg0, final Object arg1) throws Exception; 

  public void addInEdgePlaceholder(final long arg0, final long arg1, final String arg2) throws Exception; 

  public void setTempProperties(final HashMap<java.lang.Long,java.util.Map<java.lang.String, java.lang.Object>> arg0) throws Exception; 

  public Map<Peer,GraphlyCount>  countEdges(final Dir arg0, final int arg1, final long[] arg2) throws Exception; 

  public void addEdges(final long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public void addVertex(final long arg0, final String arg1) throws Exception; 

  public void addEdge(final long arg0, final long arg1, final String arg2, final Object[] arg3) throws Exception; 

  public void removeVertex(final long arg0) throws Exception; 

  public Map<Peer,long[]>  getEdges(final Dir arg0, final int arg1, final long[] arg2) throws Exception; 

  public void addRange(final int arg0) throws Exception; 

  public Map<Peer,TLongArrayList>  getVertices(final long arg0, final int arg1, final boolean arg2) throws Exception; 

  public void setEdgeProperty(final long arg0, final long arg1, final String arg2, final Object arg3, final String[] arg4) throws Exception; 

  public Map<Peer,Object>  getEdgeProperty(final long arg0, final long arg1, final String arg2, final String[] arg3) throws Exception; 

  public Map<Peer,String>  getLabel(final long arg0) throws Exception; 

  public Map<Peer,Integer>  getVertexCount() throws Exception; 

  public Map<Peer,Integer>  getEdgeCount(final long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public Map<Peer,Long>  getRandomEdge(final long arg0, final long[] arg1, final Dir arg2) throws Exception; 

  public Map<Peer,List>  getRanges() throws Exception; 

  public void commitUpdates(final String[] arg0) throws Exception; 

}