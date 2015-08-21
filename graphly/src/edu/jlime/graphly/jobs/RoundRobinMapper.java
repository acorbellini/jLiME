package edu.jlime.graphly.jobs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.util.GraphlyUtil;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

//Simple Round Robin

public class RoundRobinMapper implements Mapper {

	private static final long serialVersionUID = -2914997038447380314L;
	private ClientNode[] nodes;

	// public static void main(String[] args) {
	// HashMap<ClientNode, TIntArrayList> div = new HashMap<ClientNode,
	// TIntArrayList>();
	//
	// ArrayList<ClientNode> serverList = new ArrayList<>();
	// serverList.add(new ClientNode(new Peer("1"), null, null));
	// serverList.add(new ClientNode(new Peer("2"), null, null));
	// serverList.add(new ClientNode(new Peer("3"), null, null));
	// serverList.add(new ClientNode(new Peer("4"), null, null));
	// int[] data = new int[] { 1, 2, 3, 4 };
	// System.out.println("Mapping " + data.length + " between "
	// + serverList.size());
	// int count = 0;
	// for (int i : data) {
	// ClientNode p = serverList.get(count);
	// count = (count + 1) % serverList.size();
	// TIntArrayList uList = div.get(p);
	// if (uList == null) {
	// uList = new TIntArrayList();
	// div.put(p, uList);
	// }
	// uList.add(i);
	// }
	// System.out.println("Resulting list (size " + div.size() + ")");
	// System.out.println(div);
	// }

	@Override
	public List<Pair<ClientNode, TLongArrayList>> map(int max, long[] data,
			JobContext ctx) throws Exception {
		Logger log = Logger.getLogger(RoundRobinMapper.class);
		HashMap<ClientNode, TLongArrayList> div = new HashMap<ClientNode, TLongArrayList>();

		ArrayList<ClientNode> serverList = ctx.getCluster().getExecutors();
		if (log.isDebugEnabled())
			log.debug("Mapping " + data.length + " between " + serverList);
		int count = 0;
		for (long i : data) {
			ClientNode p = serverList.get(count);
			count = (count + 1) % serverList.size();
			TLongArrayList uList = div.get(p);
			if (uList == null) {
				uList = new TLongArrayList();
				div.put(p, uList);
			}
			uList.add(i);
		}
		if (log.isDebugEnabled())
			log.debug("Resulting list (size " + div.size() + ")");
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
		ArrayList<ClientNode> exec = ctx.getCluster().getExecutors();
		nodes = new ClientNode[exec.size()];
		int i = 0;
		for (ClientNode clientNode : exec) {
			nodes[i++] = clientNode;
		}

	}

	@Override
	public ClientNode getNode(long v, JobContext ctx) {
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
		return (int) (v % nodes.length);
	}
}
