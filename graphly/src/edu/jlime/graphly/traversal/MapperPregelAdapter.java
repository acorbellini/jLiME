package edu.jlime.graphly.traversal;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.client.JobContext;
import edu.jlime.pregel.client.SplitFunction;

public class MapperPregelAdapter implements SplitFunction, Transferible {

	private transient JobContext ctx;

	private Mapper mapper;

	public MapperPregelAdapter(Mapper mapper, RPCDispatcher rpc)
			throws Exception {
		this.mapper = mapper;
		setRPC(rpc);
	}

	@Override
	public void setRPC(RPCDispatcher rpc) throws Exception {
		Graphly g = (Graphly) ((JobDispatcher) rpc
				.getTarget(JobDispatcher.JOB_DISPATCHER)).getGlobal("graphly");

		JobDispatcher cli = g.getJobClient();

		this.ctx = cli.getEnv().getClientEnv(cli.getLocalPeer());
	}

	@Override
	public Peer getPeer(long v, List<Peer> peers) {
		return mapper.getPeer(v, this.ctx).getPeer();
	}

	@Override
	public void update() throws Exception {
		mapper.update(ctx);

	}
}
