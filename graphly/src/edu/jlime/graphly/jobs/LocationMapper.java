package edu.jlime.graphly.jobs;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public class LocationMapper implements Mapper, Closeable {

	private static final long serialVersionUID = 1634522852310272015L;

	private transient volatile Graphly g;
	private Node[] nodes;

	private transient volatile Logger log;

	private Peer[] peers;

	@Override
	public List<Pair<Node, TLongArrayList>> map(int max, long[] data, JobContext ctx) throws Exception {
		Map<Peer, TLongArrayList> map = getGraph(ctx).getHash().hashKeys(data);
		Map<Node, TLongArrayList> ret = new HashMap<>();
		for (Entry<Peer, TLongArrayList> e : map.entrySet()) {
			ret.put(getGraph(ctx).getJobClient().getCluster().getClientFor(e.getKey()), e.getValue());
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
		this.nodes = new Node[peers.length];
		for (int i = 0; i < this.nodes.length; i++) {
			this.nodes[i] = ctx.getCluster().getClientFor(this.peers[i]);
		}
	}

	@Override
	public Node getNode(long v, JobContext ctx) {
		return nodes[getGraph(ctx).getHash().hash(v)];
	}

	private Graphly getGraph(JobContext ctx) {
		if (g == null) {
			synchronized (this) {
				if (g == null) {
					this.log = Logger.getLogger(LocationMapper.class);
					this.g = (Graphly) ctx.getGlobal("graphly");
					ctx.put("location_mapper", this);
				}
			}
		}
		return g;
	}

	@Override
	public Peer[] getPeers() {
		return peers;
	}

	@Override
	public int hash(long v, JobContext ctx) {
		return getGraph(ctx).getHash().hash(v);
	}

	@Override
	public void close() throws IOException {
		synchronized (this) {
			this.g = null;
			this.nodes = null;
		}
	}
}
