package edu.jlime.graphly.traversal;

import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.Transferible;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.jd.Dispatcher;
import edu.jlime.jd.client.JobContext;
import edu.jlime.pregel.client.SplitFunction;

public class MapperPregelAdapter implements SplitFunction, Transferible {

	private transient JobContext ctx;

	private Mapper mapper;

	public MapperPregelAdapter(Mapper mapper, RPC rpc) throws Exception {
		this.mapper = mapper;
		setRPC(rpc);
	}

	@Override
	public void setRPC(RPC rpc) throws Exception {
		Graphly g = (Graphly) ((Dispatcher) rpc.getTarget(Dispatcher.JOB_DISPATCHER)).getGlobal("graphly");

		Dispatcher cli = g.getJobClient();

		this.ctx = cli.getEnv().getClientEnv(cli.getLocalPeer());
	}

	@Override
	public Peer getPeer(long v, List<Peer> peers) {
		return mapper.getNode(v, this.ctx).getPeer();
	}

	@Override
	public void update(List<Peer> peers) throws Exception {
		mapper.update(ctx);
	}

	@Override
	public Peer[] getPeers() {
		return mapper.getPeers();
	}

	@Override
	public int hash(long to) {
		return mapper.hash(to, ctx);
	}
}
