package edu.jlime.pregel.graph.rpc;

import edu.jlime.pregel.graph.rpc.Graph;
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

public class GraphServerImpl extends RPCClient implements Graph {

    RPCDispatcher local = null;
  public GraphServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public Object get(Long arg0, String arg1)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).get(arg0,arg1);
}
    return (Object) disp.callSync(dest, client, targetID, "get",new Object[] { arg0,arg1 });
  }

  public String getName()  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).getName();
}
    return (String) disp.callSync(dest, client, targetID, "getName",new Object[] {  });
  }

  public String print()  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).print();
}
    return (String) disp.callSync(dest, client, targetID, "print",new Object[] {  });
  }

  public Object getDefaultValue(String arg0)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).getDefaultValue(arg0);
}
    return (Object) disp.callSync(dest, client, targetID, "getDefaultValue",new Object[] { arg0 });
  }

  public void setVal(Long arg0, String arg1, Object arg2)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).setVal(arg0,arg1,arg2);
		return;}
    disp.callSync(dest, client, targetID, "setVal",new Object[] { arg0,arg1,arg2 });
  }

  public void disable(Long arg0)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).disable(arg0);
		return;}
    disp.callSync(dest, client, targetID, "disable",new Object[] { arg0 });
  }

	public void createVertices(Set<java.lang.Long> arg0) throws Exception  {
if(local!=null) {
((Graph) local.getTarget(targetID) ).createVertices(arg0);
		return;}
    disp.callSync(dest, client, targetID, "createVertices",new Object[] { arg0 });
  }

  public void putOutgoing(List<java.lang.Long[]> arg0)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).putOutgoing(arg0);
		return;}
    disp.callSync(dest, client, targetID, "putOutgoing",new Object[] { arg0 });
  }

  public void putOutgoing(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).putOutgoing(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "putOutgoing",new Object[] { arg0,arg1 });
  }

  public int getAdyacencySize(Long arg0)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).getAdyacencySize(arg0);
}
    return (int) disp.callSync(dest, client, targetID, "getAdyacencySize",new Object[] { arg0 });
  }

  public void putLink(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).putLink(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "putLink",new Object[] { arg0,arg1 });
  }

  public int getOutgoingSize(Long arg0)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).getOutgoingSize(arg0);
}
    return (int) disp.callSync(dest, client, targetID, "getOutgoingSize",new Object[] { arg0 });
  }

  public void disableIncoming(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).disableIncoming(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "disableIncoming",new Object[] { arg0,arg1 });
  }

  public void disableLink(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).disableLink(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "disableLink",new Object[] { arg0,arg1 });
  }

  public void disableOutgoing(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).disableOutgoing(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "disableOutgoing",new Object[] { arg0,arg1 });
  }

  public int vertexSize()  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).vertexSize();
}
    return (int) disp.callSync(dest, client, targetID, "vertexSize",new Object[] {  });
  }

  public void setDefaultValue(String arg0, Object arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).setDefaultValue(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "setDefaultValue",new Object[] { arg0,arg1 });
  }

  public Collection getOutgoing(Long arg0)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).getOutgoing(arg0);
}
    return (Collection) disp.callSync(dest, client, targetID, "getOutgoing",new Object[] { arg0 });
  }

  public boolean createVertex(Long arg0)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).createVertex(arg0);
}
    return (boolean) disp.callSync(dest, client, targetID, "createVertex",new Object[] { arg0 });
  }

  public Collection getIncoming(Long arg0)  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).getIncoming(arg0);
}
    return (Collection) disp.callSync(dest, client, targetID, "getIncoming",new Object[] { arg0 });
  }

  public void putIncoming(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).putIncoming(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "putIncoming",new Object[] { arg0,arg1 });
  }

  public void enableAll()  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).enableAll();
		return;}
    disp.callSync(dest, client, targetID, "enableAll",new Object[] {  });
  }

  public void removeOutgoing(Long arg0, Long arg1)  throws Exception {
if(local!=null) {
((Graph) local.getTarget(targetID) ).removeOutgoing(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "removeOutgoing",new Object[] { arg0,arg1 });
  }

  public Collection vertices()  throws Exception {
if(local!=null) {
		return ((Graph) local.getTarget(targetID) ).vertices();
}
    return (Collection) disp.callSync(dest, client, targetID, "vertices",new Object[] {  });
  }

}