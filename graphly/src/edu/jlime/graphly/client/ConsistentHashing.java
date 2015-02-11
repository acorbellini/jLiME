package edu.jlime.graphly.client;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import edu.jlime.core.cluster.Peer;
import edu.jlime.graphly.GraphlyStoreNodeI;
import gnu.trove.list.array.TLongArrayList;

public class ConsistentHashing implements Serializable {

	int keySpace = 50000;

	TreeMap<Integer, Peer> circle = new TreeMap<>();

	public ConsistentHashing(Map<Peer, GraphlyStoreNodeI> nodes, int vNodes)
			throws Exception {
		initCircle(nodes);
		if (circle.isEmpty()) {
			createCircle(nodes, vNodes);
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

	private void createCircle(Map<Peer, GraphlyStoreNodeI> map, int vNodes)
			throws Exception {
		ArrayList<Peer> nodes = new ArrayList<>(map.keySet());
		int points = vNodes * nodes.size();
		int range = keySpace / points;
		for (int i = 0; i < vNodes; i++) {
			Peer gs = nodes.get(i % nodes.size());
			int j = range * i;
			circle.put(j, gs);
			map.get(gs).addRange(j);
		}
	}

	Peer getNode(long k) {
		Entry<Integer, Peer> ceilingEntry = circle
				.ceilingEntry((int) ((k * 20000003) % keySpace));
		if (ceilingEntry == null)
			return circle.firstEntry().getValue();
		else
			return ceilingEntry.getValue();
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
