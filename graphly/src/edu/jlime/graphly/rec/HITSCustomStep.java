package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;

final class HITSCustomStep implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;
	private int max_edges;

	public HITSCustomStep(String auth, String hub, int steps, int max_edges) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.max_edges = max_edges;
	}

	@Override
	public TraversalResult execute(TraversalResult before,
			GraphlyTraversal tr) throws Exception {
		long[] subgraph = before.vertices().toArray();
		Graphly g = tr.getGraph();
		return g.v(subgraph).set("mapper", tr.get("mapper"))
				.repeat(steps, new HITSRepeat(auth, hub, subgraph)).exec();
	}
}