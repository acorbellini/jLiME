package edu.jlime.graphly;

import java.util.List;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import gnu.trove.map.hash.TLongIntHashMap;

public class GraphlyStoreNodeIServerImpl extends RPCClient implements GraphlyStoreNodeI, Transferible {

   Peer getJobAddressCached = null;
   transient RPCDispatcher local = null;
  public GraphlyStoreNodeIServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public void setProperty(final Long arg0, final String arg1, final Object arg2)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).setProperty(arg0,arg1,arg2);
		return;}
    disp.callSync(dest, client, targetID, "setProperty",new Object[] { arg0,arg1,arg2 });
  }

  public Object getProperty(final Long arg0, final String arg1)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getProperty(arg0,arg1);
}
    return (Object) disp.callSync(dest, client, targetID, "getProperty",new Object[] { arg0,arg1 });
  }

  public void addInEdgePlaceholder(final Long arg0, final Long arg1, final String arg2)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).addInEdgePlaceholder(arg0,arg1,arg2);
		return;}
    disp.callSync(dest, client, targetID, "addInEdgePlaceholder",new Object[] { arg0,arg1,arg2 });
  }

  public void addEdge(final Long arg0, final Long arg1, final String arg2, final Object[] arg3)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).addEdge(arg0,arg1,arg2,arg3);
		return;}
    disp.callSync(dest, client, targetID, "addEdge",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void removeVertex(final Long arg0)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).removeVertex(arg0);
		return;}
    disp.callSync(dest, client, targetID, "removeVertex",new Object[] { arg0 });
  }

  public boolean addVertex(final Long arg0, final String arg1)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).addVertex(arg0,arg1);
}
    return (boolean) disp.callSync(dest, client, targetID, "addVertex",new Object[] { arg0,arg1 });
  }

  public void addEdges(final Long arg0, final Dir arg1, final long[] arg2)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).addEdges(arg0,arg1,arg2);
		return;}
    disp.callSync(dest, client, targetID, "addEdges",new Object[] { arg0,arg1,arg2 });
  }

  public TLongIntHashMap countEdges(final Dir arg0, final long[] arg1)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).countEdges(arg0,arg1);
}
    return (TLongIntHashMap) disp.callSync(dest, client, targetID, "countEdges",new Object[] { arg0,arg1 });
  }

  public Long getRandomEdge(final Long arg0, final Dir arg1)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getRandomEdge(arg0,arg1);
}
    return (Long) disp.callSync(dest, client, targetID, "getRandomEdge",new Object[] { arg0,arg1 });
  }

  public Object getEdgeProperty(final Long arg0, final Long arg1, final String arg2, final String[] arg3)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getEdgeProperty(arg0,arg1,arg2,arg3);
}
    return (Object) disp.callSync(dest, client, targetID, "getEdgeProperty",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void setEdgeProperty(final Long arg0, final Long arg1, final String arg2, final Object arg3, final String[] arg4)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).setEdgeProperty(arg0,arg1,arg2,arg3,arg4);
		return;}
    disp.callSync(dest, client, targetID, "setEdgeProperty",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

  public long[] getEdges(final Dir arg0, final long[] arg1)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getEdges(arg0,arg1);
}
    return (long[]) disp.callSync(dest, client, targetID, "getEdges",new Object[] { arg0,arg1 });
  }

  public List getEdges(final Long arg0, final Dir arg1, final String[] arg2)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getEdges(arg0,arg1,arg2);
}
    return (List) disp.callSync(dest, client, targetID, "getEdges",new Object[] { arg0,arg1,arg2 });
  }

  public void addRange(final Integer arg0)  throws Exception {
if(local!=null) {
((GraphlyStoreNodeI) local.getTarget(targetID) ).addRange(arg0);
		return;}
    disp.callSync(dest, client, targetID, "addRange",new Object[] { arg0 });
  }

  public List getRanges()  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getRanges();
}
    return (List) disp.callSync(dest, client, targetID, "getRanges",new Object[] {  });
  }

  public String getLabel(final Long arg0)  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getLabel(arg0);
}
    return (String) disp.callSync(dest, client, targetID, "getLabel",new Object[] { arg0 });
  }

  public Peer getJobAddress()  throws Exception {
if(local!=null) {
		return ((GraphlyStoreNodeI) local.getTarget(targetID) ).getJobAddress();
}
    if (getJobAddressCached==null){
    	synchronized(this){
    		if (getJobAddressCached==null)
    			getJobAddressCached=(Peer) disp.callSync(dest, client, targetID, "getJobAddress",new Object[] {  });

    	}
    }
	return getJobAddressCached;
  }

@Override
public void setRPC(RPCDispatcher rpc) {
this.disp=rpc;
}
}