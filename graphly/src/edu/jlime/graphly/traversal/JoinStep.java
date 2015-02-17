package edu.jlime.graphly.traversal;

import java.util.Map;
import java.util.Map.Entry;

import edu.jlime.graphly.client.Graphly;
import gnu.trove.map.hash.TLongObjectHashMap;

public class JoinStep implements Step {

	private String from;
	private Join j;
	private GraphlyTraversal tr;
	private String to;

	public JoinStep(String from, String to, Join join, GraphlyTraversal tr) {
		this.from = from;
		this.to = to;
		this.j = join;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		Graphly graph = tr.getGraph();
		TLongObjectHashMap<Object> collected = graph.collect(from, before
				.vertices().toArray());
		TLongObjectHashMap<Object> map = j.join(collected);
		graph.set(to, map);
		return before;
	}

}
