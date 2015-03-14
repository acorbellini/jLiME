package edu.jlime.pregel.coordinator.rpc;

import edu.jlime.pregel.coordinator.rpc.Coordinator;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.PregelExecution;
import java.lang.Exception;
import java.util.UUID;
import java.util.UUID;
import java.lang.Boolean;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Long;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Long;
import java.lang.String;
import java.lang.Double;
import java.lang.Exception;

public class CoordinatorServerImpl extends RPCClient implements Coordinator, Transferible {

   transient RPCDispatcher local = null;
  public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public PregelExecution execute(final VertexFunction arg0, final PregelConfig arg1, final Peer arg2)  throws Exception {
if(local!=null) {
		return ((Coordinator) local.getTarget(targetID) ).execute(arg0,arg1,arg2);
}
    return (PregelExecution) disp.callSync(dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2 });
  }

  public void finished(final UUID arg0, final UUID arg1, final Boolean arg2)  throws Exception {
if(local!=null) {
async.execute(new Runnable(){
public void run(){
try{
          ((Coordinator) local.getTarget(targetID) ).finished(arg0,arg1,arg2);
} catch (Exception e) {e.printStackTrace();}}
});
;
		return;}
    disp.callAsync(dest, client, targetID, "finished",new Object[] { arg0,arg1,arg2 });
  }

  public Double getAggregatedValue(final UUID arg0, final Long arg1, final String arg2)  throws Exception {
if(local!=null) {
		return ((Coordinator) local.getTarget(targetID) ).getAggregatedValue(arg0,arg1,arg2);
}
    return (Double) disp.callSync(dest, client, targetID, "getAggregatedValue",new Object[] { arg0,arg1,arg2 });
  }

  public void setAggregatedValue(final UUID arg0, final Long arg1, final String arg2, final Double arg3)  throws Exception {
if(local!=null) {
((Coordinator) local.getTarget(targetID) ).setAggregatedValue(arg0,arg1,arg2,arg3);
		return;}
    disp.callSync(dest, client, targetID, "setAggregatedValue",new Object[] { arg0,arg1,arg2,arg3 });
  }

@Override
public void setRPC(RPCDispatcher rpc) {
this.disp=rpc;
}
}