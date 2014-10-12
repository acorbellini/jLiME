package edu.jlime.jd;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.jd.rpc.JobExecutor;

public class JobExecutorFactory {

	private RPCDispatcher rpc;

	private String target;

	public JobExecutorFactory(RPCDispatcher rpc, String target) {
		this.rpc = rpc;
		this.target = target;
	}

	public JobExecutorBroadcast getBroadcast(List<Peer> to, Peer cliID) {
		return new JobExecutorBroadcastImpl(rpc, to, cliID, target);
	}

	public JobExecutor get(Peer to, Peer cliID) {
		return new JobExecutorServerImpl(rpc, to, cliID, target);
	}
}