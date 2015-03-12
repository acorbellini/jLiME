package edu.jlime.graphly.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.GraphlyStoreNodeI;
import gnu.trove.list.array.TLongArrayList;

public class ConsistentHashing implements Serializable {

	int keySpace = 50021;

	HashMap<Integer, Peer> circle = new HashMap<>();

	Peer firstPeer;

	private int vnodes;

	private int range;

	public ConsistentHashing(Map<Peer, GraphlyStoreNodeI> nodes, int vNodes)
			throws Exception {
		this.vnodes = vNodes;
		this.range = keySpace / vNodes;
		initCircle(nodes);
		if (circle.isEmpty()) {
			createCircle(nodes);
		}
	}

	private void initCircle(Map<Peer, GraphlyStoreNodeI> nodes)
			throws Exception {
		for (Peer gn : nodes.keySet()) {
			for (Integer range : nodes.get(gn).getRanges()) {
				circle.put(range, gn);
			}
		}
	}

	private void createCircle(Map<Peer, GraphlyStoreNodeI> map)
			throws Exception {
		ArrayList<Peer> nodes = new ArrayList<>(map.keySet());
		for (int i = 0; i < vnodes; i++) {
			Peer gs = nodes.get(i % nodes.size());
			int j = range * i;
			circle.put(j, gs);
			map.get(gs).addRange(j);
		}
	}

	public Peer getNode(long k) {
		int hashed = Math.abs(((int) k * 31) % vnodes);
		int vNode = (hashed + 1) * range;
		Peer ceilingEntry = circle.get(vNode);
		if (ceilingEntry == null)
			return circle.get(0);
		return ceilingEntry;
	}

	public Map<Peer, TLongArrayList> hashKeys(long[] data) {
		Map<Peer, TLongArrayList> ret = new HashMap<>();
		for (long l : data) {
			Peer node = getNode(l);
			TLongArrayList curr = ret.get(node);
			if (curr == null) {
				curr = new TLongArrayList();
				ret.put(node, curr);
			}
			curr.add(l);
		}
		return ret;
	}

}
