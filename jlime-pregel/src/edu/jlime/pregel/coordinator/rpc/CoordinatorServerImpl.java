package edu.jlime.pregel.coordinator.rpc;

import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.pregel.PregelExecution;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public class CoordinatorServerImpl extends RPCClient implements Coordinator {

    RPCDispatcher local = null;
  public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public PregelExecution execute(VertexFunction arg0, PregelConfig arg1, Peer arg2)  throws Exception {
if(local!=null) {
		return ((Coordinator) local.getTarget(targetID) ).execute(arg0,arg1,arg2);
}
    return (PregelExecution) disp.callSync(dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2 });
  }

  public Double getAggregatedValue(UUID arg0, Long arg1, String arg2)  throws Exception {
if(local!=null) {
		return ((Coordinator) local.getTarget(targetID) ).getAggregatedValue(arg0,arg1,arg2);
}
    return (Double) disp.callSync(dest, client, targetID, "getAggregatedValue",new Object[] { arg0,arg1,arg2 });
  }

  public void setAggregatedValue(UUID arg0, Long arg1, String arg2, Double arg3)  throws Exception {
if(local!=null) {
((Coordinator) local.getTarget(targetID) ).setAggregatedValue(arg0,arg1,arg2,arg3);
		return;}
    disp.callSync(dest, client, targetID, "setAggregatedValue",new Object[] { arg0,arg1,arg2,arg3 });
  }

  public void finished(UUID arg0, UUID arg1, Boolean arg2)  throws Exception {
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

}