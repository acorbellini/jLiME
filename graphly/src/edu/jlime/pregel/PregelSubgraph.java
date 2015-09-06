package edu.jlime.pregel;

import java.util.concurrent.ConcurrentHashMap;

import edu.jlime.pregel.graph.rpc.Graph;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class PregelSubgraph {
	ConcurrentHashMap<Long, long[]> in = new ConcurrentHashMap<>();
	ConcurrentHashMap<Long, long[]> out = new ConcurrentHashMap<>();
	private TLongHashSet subgraph;

	private Graph graph;

	public PregelSubgraph(TLongHashSet value, Graph graph) {
		this.subgraph = value;
		this.graph = graph;
	}

	public long[] getOutgoing(long v) throws Exception {
		long[] list = out.get(v);
		if (list == null) {
			synchronized (out) {
				list = out.get(v);
				if (list == null) {
					TLongHashSet s = new TLongHashSet(graph.getOutgoing(v));
					TLongIterator it = s.iterator();
					while (it.hasNext()) {
						long sV = it.next();
						if (!subgraph.contains(sV))
							it.remove();
					}

					list = s.toArray();
					out.put(v, list);
				}
			}
		}
		return list;
	}

	public long[] getIncoming(long v) throws Exception {
		long[] list = in.get(v);
		if (list == null) {
			synchronized (in) {
				list = in.get(v);
				if (list == null) {
					TLongHashSet s = new TLongHashSet(graph.getIncoming(v));
					TLongIterator it = s.iterator();
					while (it.hasNext()) {
						long sV = it.next();
						if (!subgraph.contains(sV))
							it.remove();
					}
					list = s.toArray();
					in.put(v, list);
				}
			}
		}
		return list;
	}

	public int size() {
		return subgraph.size();
	}
}
