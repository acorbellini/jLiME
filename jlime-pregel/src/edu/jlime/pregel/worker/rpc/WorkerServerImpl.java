package edu.jlime.pregel.worker.rpc;

import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import java.util.UUID;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Exception;
import java.util.UUID;
import edu.jlime.core.cluster.Peer;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.client.PregelConfig;
import java.util.Set;
import java.lang.Exception;
import java.lang.Integer;
import java.util.UUID;
import java.lang.Exception;
import edu.jlime.pregel.worker.PregelMessage;
import java.util.UUID;
import java.lang.Exception;
import java.util.List;
import java.util.UUID;
import java.lang.Exception;

public class WorkerServerImpl extends RPCClient implements Worker, Transferible {

   UUID getIDCached = null;
   transient RPCDispatcher local = null;
  public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public void execute(final UUID arg0)  throws Exception {
if(local!=null) {
async.execute(new Runnable(){
public void run(){
try{
          ((Worker) local.getTarget(targetID) ).execute(arg0);
} catch (Exception e) {e.printStackTrace();}}
});
;
		return;}
    disp.callAsync(dest, client, targetID, "execute",new Object[] { arg0 });
  }

  public UUID getID()  throws Exception {
if(local!=null) {
		return ((Worker) local.getTarget(targetID) ).getID();
}
    if (getIDCached==null){
    	synchronized(this){
    		if (getIDCached==null)
    			getIDCached=(UUID) disp.callSync(dest, client, targetID, "getID",new Object[] {  });

    	}
    }
	return getIDCached;
  }

  public void createTask(final UUID arg0, final Peer arg1, final VertexFunction arg2, final PregelConfig arg3, final Set<java.lang.Long> arg4)  throws Exception {
if(local!=null) {
((Worker) local.getTarget(targetID) ).createTask(arg0,arg1,arg2,arg3,arg4);
		return;}
    disp.callSync(dest, client, targetID, "createTask",new Object[] { arg0,arg1,arg2,arg3,arg4 });
  }

  public void nextSuperstep(final Integer arg0, final UUID arg1)  throws Exception {
if(local!=null) {
((Worker) local.getTarget(targetID) ).nextSuperstep(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "nextSuperstep",new Object[] { arg0,arg1 });
  }

  public void sendMessage(final PregelMessage arg0, final UUID arg1)  throws Exception {
if(local!=null) {
((Worker) local.getTarget(targetID) ).sendMessage(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "sendMessage",new Object[] { arg0,arg1 });
  }

  public void sendMessages(final List<edu.jlime.pregel.worker.PregelMessage> arg0, final UUID arg1)  throws Exception {
if(local!=null) {
((Worker) local.getTarget(targetID) ).sendMessages(arg0,arg1);
		return;}
    disp.callSync(dest, client, targetID, "sendMessages",new Object[] { arg0,arg1 });
  }

@Override
public void setRPC(RPCDispatcher rpc) {
this.disp=rpc;
}
}