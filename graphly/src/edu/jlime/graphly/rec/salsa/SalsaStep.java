package edu.jlime.graphly.rec.salsa;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.MinEdgeFilter;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.list.array.TLongArrayList;

public class SalsaStep implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;
	private int max_depth;

	public SalsaStep(String auth, String hub, int steps, int max_depth) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.max_depth = max_depth;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		long[] subgraph = before.vertices().toArray();

		Graphly g = tr.getGraph();
		TLongArrayList authSet = g.v(subgraph)
				.filter(new MinEdgeFilter(Dir.IN, 1, subgraph)).exec()
				.vertices();
		TLongArrayList hubSet = g.v(subgraph)
				.filter(new MinEdgeFilter(Dir.OUT, 1, subgraph)).exec()
				.vertices();
		return g.v(subgraph).set("mapper", tr.get("mapper"))
				.repeat(steps, new SalsaRepeat(auth, hub, authSet, hubSet))
				.exec();
	}
}