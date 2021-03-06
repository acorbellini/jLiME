package edu.jlime.jd.rpc;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.ClientFactory;
import edu.jlime.core.rpc.RPC;

public class JobExecutorFactory implements ClientFactory<JobExecutor, JobExecutorBroadcast> {
	private RPC rpc;
	private String target;

	public JobExecutorFactory(RPC rpc, String target) {
		this.rpc = rpc;
		this.target = target;
	}

	public JobExecutorBroadcast getBroadcast(List<Peer> to, Peer client) {
		return new JobExecutorBroadcastImpl(rpc, to, client, target);
	}

	public JobExecutor get(Peer to, Peer client) {
		return new JobExecutorServerImpl(rpc, to, client, target);
	}
}
