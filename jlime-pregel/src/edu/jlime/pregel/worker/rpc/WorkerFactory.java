package edu.jlime.pregel.worker.rpc;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPCDispatcher;

public class WorkerFactory implements ClientFactory<Worker, WorkerBroadcast> {
	private RPCDispatcher rpc;
	private String target;

	public WorkerFactory(RPCDispatcher rpc, String target) {
		this.rpc = rpc;
		this.target = target;
	}

	public WorkerBroadcast getBroadcast(List<Peer> to, Peer client) {
		return new WorkerBroadcastImpl(rpc, to, client, target);
	}

	public Worker get(Peer to, Peer client) {
		return new WorkerServerImpl(rpc, to, client, target);
	}
}
