package edu.jlime.pregel.worker;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.rpc.RPCDispatcher;

public class WorkerServerImpl extends RPCClient implements Worker {

	public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
	}

	public void sendDataToVertex(int arg0, byte[] arg1) throws Exception {
		disp.callAsync(dest, client, targetID, "sendDataToVertex",
				new Object[] { arg0, arg1 });
	}

	public void nextSuperstep(int arg0) throws Exception {
		disp.callAsync(dest, client, targetID, "nextSuperstep",
				new Object[] { arg0 });
	}

	public void schedule(int arg0, VertexFunction arg1) throws Exception {
		disp.callAsync(dest, client, targetID, "schedule", new Object[] { arg0,
				arg1 });
	}

}