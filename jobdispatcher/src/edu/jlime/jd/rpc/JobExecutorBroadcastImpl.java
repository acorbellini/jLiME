package edu.jlime.jd.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.JobContainer;

public class JobExecutorBroadcastImpl implements JobExecutorBroadcast {

  RPCDispatcher disp;
  Peer local;
  List<Peer> dest = new ArrayList<Peer>();
  Peer client;
  String targetID;

  public JobExecutorBroadcastImpl(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {
    this.disp = disp;
    this.dest.addAll(dest);
    this.client = client;
    this.targetID = targetID;
  }

  public void execute(final JobContainer arg0) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "execute",new Object[] { arg0 });
  }

  public void result(final Object arg0, final UUID arg1, final ClientNode arg2) throws Exception {
    disp.multiCallAsync( dest, client, targetID, "result",new Object[] { arg0,arg1,arg2 });
  }

}