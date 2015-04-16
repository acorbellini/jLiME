package edu.jlime.graphly.jobs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import gnu.trove.list.array.TLongArrayList;

public class LocationMapper implements Mapper {

	private static final long serialVersionUID = 1634522852310272015L;

	private transient volatile Graphly g;
	private ClientNode[] nodes;

	private transient volatile Logger log;

	private Peer[] peers;

	@Override
	public List<Pair<ClientNode, TLongArrayList>> map(int max, long[] data,
			JobContext ctx) throws Exception {

		if (log.isDebugEnabled())
			log.debug("Mapping " + data.length + " keys by location.");

		Map<Peer, TLongArrayList> map = getGraph(ctx).getHash().hashKeys(data);

		Map<ClientNode, TLongArrayList> ret = new HashMap<>();
		for (Entry<Peer, TLongArrayList> e : map.entrySet()) {
			ret.put(getGraph(ctx).getJobClient().getCluster()
					.getClientFor(e.getKey()), e.getValue());
		}
		return GraphlyUtil.divide(ret, max);
	}

	@Override
	public String getName() {
		return "location";
	}

	@Override
	public boolean isDynamic() {
		return false;
	}

	@Override
	public synchronized void update(JobContext ctx) throws Exception {
		if (nodes != null)
			return;

		this.peers = getGraph(ctx).getHash().getCircle();
		this.nodes = new ClientNode[peers.length];
		for (int i = 0; i < this.nodes.length; i++) {
			this.nodes[i] = ctx.getCluster().getClientFor(this.peers[i]);
		}
	}

	@Override
	public ClientNode getNode(long v, JobContext ctx) {
		return nodes[getGraph(ctx).getHash().hash(v)];
	}

	private Graphly getGraph(JobContext ctx) {
		if (g == null) {
			synchronized (this) {
				if (g == null) {
					this.log = Logger.getLogger(LocationMapper.class);
					this.g = (Graphly) ctx.getGlobal("graphly");
				}
			}
		}
		return g;
	}

	@Override
	public Peer[] getPeers() {
		// Peer[] peers = new Peer[nodes.length];
		// for (int i = 0; i < nodes.length; i++) {
		// peers[i] = nodes[i].getPeer();
		// }
		// return peers;

		return peers;
	}

	@Override
	public int hash(long v) {
		return g.getHash().hash(v);
	}
}
