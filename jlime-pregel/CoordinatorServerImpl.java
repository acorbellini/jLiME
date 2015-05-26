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
import java.lang.Boolean;
import java.util.HashMap;
import java.lang.Exception;

public class CoordinatorServerImpl extends RPCClient implements Coordinator, Transferible {

   transient RPCDispatcher localRPC;
   transient volatile Coordinator local = null;
  public CoordinatorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 this.localRPC = RPCDispatcher.getLocalDispatcher(dest);}

   public PregelExecution execute(final VertexFunction<?> arg0, final long[] arg1, final PregelConfig arg2, final Peer arg3)  throws Exception {
if(localRPC!=null) {
		return getLocal().execute(arg0,arg1,arg2,arg3);
}
    return (PregelExecution) disp.callSync(dest, client, targetID, "execute",new Object[] { arg0,arg1,arg2,arg3 });
  }

   public void finished(final int arg0, final UUID arg1, final Boolean arg2, final HashMap<?,?> arg3)  throws Exception {
if(localRPC!=null) {
async.execute(new Runnable(){
public void run(){
try{
          getLocal().finished(arg0,arg1,arg2,arg3);
} catch (Exception e) {e.printStackTrace();}}
});
;
		return;}
    disp.callAsync(dest, client, targetID, "finished",new Object[] { arg0,arg1,arg2,arg3 });
  }

@Override
public void setRPC(RPCDispatcher rpc) {
this.disp=rpc;
this.localRPC = RPCDispatcher.getLocalDispatcher(super.dest);
}
public Coordinator getLocal() throws Exception {	if(local==null){		synchronized(this){			if(local==null){				this.local = (Coordinator							  ) localRPC.getTarget(targetID);
			}		}}
return this.local;
}
}