package edu.jlime.graphly;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.graphly.GraphlyStoreNodeI;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.lang.Long;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Long;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.String;
import java.lang.Integer;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Integer;
import edu.jlime.graphly.GraphlyCount;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import edu.jlime.core.cluster.Peer;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.String;
import java.lang.Object;
import java.lang.String;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Long;
import java.lang.String;
import java.lang.Exception;
import java.lang.Long;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.Long;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.Long;
import java.lang.String;
import java.lang.Exception;
import java.lang.Long;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Integer;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Integer;
import java.lang.Exception;

public interface GraphlyStoreNodeIBroadcast { 

  public void setProperty(final Long arg0, final String arg1, final Object arg2) throws Exception; 

  public Map<Peer,Object>  getProperty(final Long arg0, final String arg1) throws Exception; 

  public Map<Peer,TLongObjectHashMap>  getProperties(final String arg0, final Integer arg1, final TLongArrayList arg2) throws Exception; 

  public void setProperties(final String arg0, final TLongObjectHashMap<java.lang.Object> arg1) throws Exception; 

  public void addInEdgePlaceholder(final Long arg0, final Long arg1, final String arg2) throws Exception; 

  public Map<Peer,GraphlyCount>  countEdges(final Dir arg0, final Integer arg1, final long[] arg2) throws Exception; 

  public void removeVertex(final Long arg0) throws Exception; 

  public void addEdge(final Long arg0, final Long arg1, final String arg2, final Object[] arg3) throws Exception; 

  public Map<Peer,List>  getRanges() throws Exception; 

  public Map<Peer,Peer>  getJobAddress() throws Exception; 

  public void setEdgeProperty(final Long arg0, final Long arg1, final String arg2, final Object arg3, final String[] arg4) throws Exception; 

  public Map<Peer,Object>  getEdgeProperty(final Long arg0, final Long arg1, final String arg2, final String[] arg3) throws Exception; 

  public Map<Peer,String>  getLabel(final Long arg0) throws Exception; 

  public Map<Peer,Integer>  getEdgeCount(final Long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public void addEdges(final Long arg0, final Dir arg1, final long[] arg2) throws Exception; 

  public Map<Peer,Boolean>  addVertex(final Long arg0, final String arg1) throws Exception; 

  public Map<Peer,Long>  getRandomEdge(final Long arg0, final long[] arg1, final Dir arg2) throws Exception; 

  public void addRange(final Integer arg0) throws Exception; 

  public Map<Peer,long[]>  getEdges(final Dir arg0, final Integer arg1, final long[] arg2) throws Exception; 

}