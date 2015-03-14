package edu.jlime.jd.rpc;

import edu.jlime.core.cluster.BroadcastException;import edu.jlime.jd.rpc.JobExecutor;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.jd.JobContainer;
import java.lang.Exception;
import java.lang.Object;
import java.util.UUID;
import edu.jlime.jd.ClientNode;
import java.lang.Exception;

public interface JobExecutorBroadcast { 

  public void execute(final JobContainer arg0) throws Exception; 

  public void result(final Object arg0, final UUID arg1, final ClientNode arg2) throws Exception; 

}