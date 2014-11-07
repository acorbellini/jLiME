package edu.jlime.jd.rpc;

import edu.jlime.jd.rpc.JobExecutor;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.jd.JobContainer;
import java.lang.Exception;
import java.lang.Object;
import java.util.UUID;
import edu.jlime.jd.ClientNode;
import java.lang.Exception;

public class JobExecutorServerImpl extends RPCClient implements JobExecutor {

    RPCDispatcher local = null;
  public JobExecutorServerImpl(RPCDispatcher disp, Peer dest, Peer client, String targetID) {
 super(disp, dest, client, targetID);
 local = RPCDispatcher.getLocalDispatcher(dest);
}

  public void execute(final JobContainer arg0)  throws Exception {
if(local!=null) {
async.execute(new Runnable(){
public void run(){
try{
          ((JobExecutor) local.getTarget(targetID) ).execute(arg0);
} catch (Exception e) {e.printStackTrace();}}
});
;
		return;}
    disp.callAsync(dest, client, targetID, "execute",new Object[] { arg0 });
  }

  public void result(final Object arg0, final UUID arg1, final ClientNode arg2)  throws Exception {
if(local!=null) {
async.execute(new Runnable(){
public void run(){
try{
          ((JobExecutor) local.getTarget(targetID) ).result(arg0,arg1,arg2);
} catch (Exception e) {e.printStackTrace();}}
});
;
		return;}
    disp.callAsync(dest, client, targetID, "result",new Object[] { arg0,arg1,arg2 });
  }

}