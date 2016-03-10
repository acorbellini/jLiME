package edu.jlime.graphly.jobs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

//Simple Round Robin

public class RoundRobinMapper implements Mapper {
	private static final int PRIME = 1;

	private static final long serialVersionUID = -2914997038447380314L;
	private Node[] nodes;

	@Override
	public List<Pair<Node, TLongArrayList>> map(int max, long[] data, JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(RoundRobinMapper.class);
		HashMap<Node, TLongArrayList> div = new HashMap<Node, TLongArrayList>();

		update(ctx);

		// if (log.isDebugEnabled())
		log.info("Mapping " + data.length + " between " + nodes.length);
		for (long i : data) {
			Node p = getNode(i, ctx);
			TLongArrayList uList = div.get(p);
			if (uList == null) {
				uList = new TLongArrayList();
				div.put(p, uList);
			}
			uList.add(i);
		}
		// if (log.isDebugEnabled())
		log.info("Resulting list (size " + div.size() + ")");
		return GraphlyUtil.divide(div, max);
	}

	@Override
	public String getName() {
		return "roundrobin";
	}

	@Override
	public boolean isDynamic() {
		return false;
	}

	@Override
	public void update(JobContext ctx) throws Exception {
		ArrayList<Node> exec = ctx.getCluster().getExecutors();
		Collections.sort(exec, new Comparator<Node>() {

			@Override
			public int compare(Node o1, Node o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		nodes = new Node[exec.size()];
		int i = 0;
		for (Node clientNode : exec) {
			nodes[i++] = clientNode;
		}

	}

	@Override
	public Node getNode(long v, JobContext ctx) {
		return nodes[hash(v, ctx)];

	}

	@Override
	public Peer[] getPeers() {
		Peer[] peers = new Peer[nodes.length];
		for (int i = 0; i < nodes.length; i++) {
			peers[i] = nodes[i].getPeer();
		}
		return peers;
	}

	@Override
	public int hash(long v, JobContext ctx) {
		return Math.abs((int) ((v * PRIME) % nodes.length));
	}
}
