package edu.jlime.pregel.graph.rpc;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.pregel.graph.rpc.Graph;
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
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.util.Collection;
import java.lang.Exception;
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
import java.lang.String;
import java.lang.Object;
import java.lang.Exception;
import java.lang.Long;
import java.util.Collection;
import java.lang.Exception;
import java.lang.Long;
import java.util.Collection;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Exception;
import java.lang.Exception;
import java.lang.Long;
import java.lang.Long;
import java.lang.Exception;
import java.util.Set;
import java.lang.Exception;

public class GraphBroadcastImpl implements GraphBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public GraphBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public Map<Peer,Object>  get(final Long arg0, final String arg1) throws Exception {
    return disp.multiCall( dest, client, targetID, "get",new Object[] { arg0,arg1 });
  }

  public Map<Peer,String>  getName() throws Exception {
    return disp.multiCall( dest, client, targetID, "getName",new Object[] {  });
  }

  public Map<Peer,String>  print() throws Exception {
    return disp.multiCall( dest, client, targetID, "print",new Object[] {  });
  }

  public Map<Peer,Object>  getDefaultValue(final String arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getDefaultValue",new Object[] { arg0 });
  }

  public void setVal(final Long arg0, final String arg1, final Object arg2) throws Exception {
    disp.multiCall( dest, client, targetID, "setVal",new Object[] { arg0,arg1,arg2 });
  }

  public void disable(final Long arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "disable",new Object[] { arg0 });
  }

  public void putIncoming(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "putIncoming",new Object[] { arg0,arg1 });
  }

  public void disableIncoming(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "disableIncoming",new Object[] { arg0,arg1 });
  }

  public void disableLink(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "disableLink",new Object[] { arg0,arg1 });
  }

  public Map<Peer,Integer>  getAdyacencySize(final Long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getAdyacencySize",new Object[] { arg0 });
  }

  public void enableAll() throws Exception {
    disp.multiCall( dest, client, targetID, "enableAll",new Object[] {  });
  }

  public void removeOutgoing(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "removeOutgoing",new Object[] { arg0,arg1 });
  }

  public Map<Peer,Collection>  vertices() throws Exception {
    return disp.multiCall( dest, client, targetID, "vertices",new Object[] {  });
  }

  public void putOutgoing(final List<java.lang.Long[]> arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "putOutgoing",new Object[] { arg0 });
  }

  public void putOutgoing(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "putOutgoing",new Object[] { arg0,arg1 });
  }

  public Map<Peer,Integer>  getOutgoingSize(final Long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getOutgoingSize",new Object[] { arg0 });
  }

  public void disableOutgoing(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "disableOutgoing",new Object[] { arg0,arg1 });
  }

  public void setDefaultValue(final String arg0, final Object arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "setDefaultValue",new Object[] { arg0,arg1 });
  }

  public Map<Peer,Collection>  getOutgoing(final Long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getOutgoing",new Object[] { arg0 });
  }

  public Map<Peer,Collection>  getIncoming(final Long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "getIncoming",new Object[] { arg0 });
  }

  public Map<Peer,Boolean>  createVertex(final Long arg0) throws Exception {
    return disp.multiCall( dest, client, targetID, "createVertex",new Object[] { arg0 });
  }

  public Map<Peer,Integer>  vertexSize() throws Exception {
    return disp.multiCall( dest, client, targetID, "vertexSize",new Object[] {  });
  }

  public void putLink(final Long arg0, final Long arg1) throws Exception {
    disp.multiCall( dest, client, targetID, "putLink",new Object[] { arg0,arg1 });
  }

  public void createVertices(final Set<java.lang.Long> arg0) throws Exception {
    disp.multiCall( dest, client, targetID, "createVertices",new Object[] { arg0 });
  }

}