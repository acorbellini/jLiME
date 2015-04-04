package edu.jlime.graphly.traversal;

import edu.jlime.graphly.client.GraphlyGraph;
import gnu.trove.map.hash.TLongObjectHashMap;

public class JoinStep<T, O> implements Step {

	private String from;
	private Join<T, O> j;
	private GraphlyTraversal tr;
	private String to;

	public JoinStep(String from, String to, Join<T, O> join, GraphlyTraversal tr) {
		this.from = from;
		this.to = to;
		this.j = join;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		GraphlyGraph graph = tr.getGraph();
		TLongObjectHashMap<T> collected = (TLongObjectHashMap<T>) graph
				.collect(from, -1, before.vertices().toArray());
		TLongObjectHashMap<O> map = j.join(collected);
		graph.set(to, (TLongObjectHashMap<Object>) map);
		return before;
	}

}
