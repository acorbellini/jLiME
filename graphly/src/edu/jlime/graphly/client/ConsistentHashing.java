package edu.jlime.graphly.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.storenode.rpc.StoreNode;
import gnu.trove.list.array.TLongArrayList;

public class ConsistentHashing implements Serializable {

	int keySpace = 50021;

	Peer[] circle;

	StoreNode[] stores;

	Peer firstPeer;

	private int vnodes;

	public ConsistentHashing(Map<Peer, StoreNode> nodes, int vNodes)
			throws Exception {
		this.vnodes = vNodes;
		circle = new Peer[vnodes];
		stores = new StoreNode[vnodes];
		if (!initCircle(nodes)) {
			createCircle(nodes);
		}
	}

	private boolean initCircle(Map<Peer, StoreNode> nodes) throws Exception {
		boolean mod = false;
		for (Peer gn : nodes.keySet()) {
			for (Integer range : nodes.get(gn).getRanges()) {
				circle[range] = gn;
				stores[range] = nodes.get(gn);
				mod = true;
			}
		}
		return mod;
	}

	private void createCircle(Map<Peer, StoreNode> map) throws Exception {
		ArrayList<Peer> nodes = new ArrayList<>(map.keySet());
		for (int i = 0; i < vnodes; i++) {
			Peer gs = nodes.get(i % nodes.size());
			circle[i] = gs;
			stores[i] = map.get(gs);
			map.get(gs).addRange(i);
		}
	}

	public Peer getNode(long k) {
		return circle[hash(k)];
	}

	public StoreNode getStore(long k) {
		return stores[hash(k)];
	}

	public int hash(long k) {
		return Math.abs(((int) k * 31) % vnodes);
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

	public int getSize() {
		return circle.length;

	}

	public Peer[] getCircle() {
		return circle;
	}

}
