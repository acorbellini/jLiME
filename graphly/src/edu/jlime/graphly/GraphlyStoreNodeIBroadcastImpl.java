package edu.jlime.graphly;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.graphly.GraphlyStoreNodeI;
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
import java.util.Map;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import gnu.trove.map.hash.TLongObjectHashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.util.HashMap;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.String;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Exception;
import java.util.List;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import edu.jlime.core.cluster.Peer;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.GraphlyCount;
import java.lang.Exception;
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import edu.jlime.graphly.traversal.Dir;
import java.lang.Exception;
import java.lang.String;
import java.lang.Exception;

public class GraphlyStoreNodeIBroadcastImpl implements GraphlyStoreNodeIBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public GraphlyStoreNodeIBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public void setProperty(final long arg0, final String arg1, final Object arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setProperty",new Object[] { arg0,arg1,arg2 });
  }

  public Map<Peer,Object>  getProperty(final long arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getProperty",new Object[] { arg0,arg1 });
  }

  public Map<Peer,Map>  getProperties(final long[] arg0, final String[] arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "getProperties",new Object[] { arg0,arg1 });
  }

  public Map<Peer,TLongObjectHashMap>  getProperties(final String arg0, final int arg1, final TLongArrayList arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getProperties",new Object[] { arg0,arg1,arg2 });
  }

  public void setProperties(final String arg0, final TLongObjectHashMap<java.lang.Object> arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "setProperties",new Object[] { arg0,arg1 });
  }

  public void addInEdgePlaceholder(final long arg0, final long arg1, final String arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "addInEdgePlaceholder",new Object[] { arg0,arg1,arg2 });
  }

  public void setTempProperties(final HashMap<java.lang.Long,java.util.Map<java.lang.String, java.lang.Object>> arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "setTempProperties",new Object[] { arg0 });
  }

  public Map<Peer,Boolean>  addVertex(final long arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "addVertex",new Object[] { arg0,arg1 });
  }

  public void setEdgeProperty(final long arg0, final long arg1, final String arg2, final Object arg3, final String[] arg4) throws Exception {
    disp.multiCall( dest, client, targetID, "setEdgeProperty",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

  public void addRange(final int arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "addRange",new Object[] { arg0 });
  }

  public void removeVertex(final long arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "removeVertex",new Object[] { arg0 });
  }

  public Map<Peer,List>  getRanges() throws Exception {
    return disp.multiCall( dest, client, targetID, "getRanges",new Object[] {  });
  }

  public Map<Peer,Integer>  getEdgeCount(final long arg0, final Dir arg1, final long[] arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdgeCount",new Object[] { arg0,arg1,arg2 });
  }

  public Map<Peer,Object>  getEdgeProperty(final long arg0, final long arg1, final String arg2, final String[] arg3) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdgeProperty",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public Map<Peer,long[]>  getEdges(final Dir arg0, final int arg1, final long[] arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getEdges",new Object[] { arg0,arg1,arg2 });
  }

  public Map<Peer,Peer>  getJobAddress() throws Exception {
    return disp.multiCall( dest, client, targetID, "getJobAddress",new Object[] {  });
  }

  public Map<Peer,Long>  getRandomEdge(final long arg0, final long[] arg1, final Dir arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "getRandomEdge",new Object[] { arg0,arg1,arg2 });
  }

  public Map<Peer,String>  getLabel(final long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getLabel",new Object[] { arg0 });
  }

  public Map<Peer,GraphlyCount>  countEdges(final Dir arg0, final int arg1, final long[] arg2) throws Exception {
    return disp.multiCall( dest, client, targetID, "countEdges",new Object[] { arg0,arg1,arg2 });
  }

  public void addEdge(final long arg0, final long arg1, final String arg2, final Object[] arg3) throws Exception {
    disp.multiCall( dest, client, targetID, "addEdge",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void addEdges(final long arg0, final Dir arg1, final long[] arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "addEdges",new Object[] { arg0,arg1,arg2 });
  }

  public void commitUpdates(final String[] arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "commitUpdates",new Object[] { arg0 });
  }

}