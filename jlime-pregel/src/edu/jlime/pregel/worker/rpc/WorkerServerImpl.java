package edu.jlime.pregel.worker.rpc;

import edu.jlime.pregel.worker.rpc.Worker;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCClient;
import edu.jlime.core.cluster.Peer;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import edu.jlime.pregel.graph.PregelGraph;
import java.lang.Exception;
import java.util.UUID;
import java.lang.Exception;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.worker.VertexData;
import java.util.UUID;
import java.lang.Exception;
import java.lang.Integer;
import java.util.UUID;
import java.lang.Exception;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.VertexFunction;
import java.util.UUID;
import java.util.HashSet;
import java.lang.Exception;

public class WorkerServerImpl extends RPCClient implements Worker {

	UUID getIDCached = null;

	public WorkerServerImpl(RPCDispatcher disp, Peer dest, Peer client,
			String targetID) {
		super(disp, dest, client, targetID);
	}

	public PregelGraph getResult(UUID arg0) throws Exception {
		return (PregelGraph) disp.callSync(dest, client, targetID, "getResult",
				new Object[] { arg0 });
	}

	public UUID getID() throws Exception {
		if (getIDCached == null) {
			synchronized (this) {
				if (getIDCached == null)
					getIDCached = (UUID) disp.callSync(dest, client, targetID,
							"getID", new Object[] {});

			}
		}
		return getIDCached;
	}

	public void sendDataToVertex(Vertex arg0, Vertex arg1, VertexData arg2,
			UUID arg3) throws Exception {
		disp.callSync(dest, client, targetID, "sendDataToVertex", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

	public void nextSuperstep(Integer arg0, UUID arg1) throws Exception {
		disp.callAsync(dest, client, targetID, "nextSuperstep", new Object[] {
				arg0, arg1 });
	}

	public void createTask(PregelGraph arg0, VertexFunction arg1, UUID arg2,
			HashSet<edu.jlime.pregel.graph.Vertex> arg3) throws Exception {
		disp.callSync(dest, client, targetID, "createTask", new Object[] {
				arg0, arg1, arg2, arg3 });
	}

}