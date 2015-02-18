package edu.jlime.graphly.rec.salsa;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.MinEdgeFilter;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;

public class SalsaStepRandomWalk implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;
	private int max_depth;

	public SalsaStepRandomWalk(String auth, String hub, int steps, int max_depth) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.max_depth = max_depth;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		long[] res = before.vertices().toArray();

		Graphly g = tr.getGraph();

		long[] authSet = g.v(res).filter(new MinEdgeFilter(Dir.IN, 1, res))
				.exec().vertices().toArray();
		TraversalResult authrw = g.v(authSet).set("mapper", tr.get("mapper"))
				.as(Recommendation.class)
				.randomwalk(auth, steps, max_depth, authSet, Dir.IN, Dir.OUT)
				.asTraversal().join(auth, auth, new SalsaJoin()).exec();

		long[] hubSet = g.v(res).filter(new MinEdgeFilter(Dir.OUT, 1, res))
				.exec().vertices().toArray();
		TraversalResult hubrw = g.v(hubSet).set("mapper", tr.get("mapper"))
				.as(Recommendation.class)
				.randomwalk(hub, steps, max_depth, hubSet, Dir.OUT, Dir.IN)
				.asTraversal().join(hub, hub, new SalsaJoin()).exec();

		tr.set("auth", g.collect(auth, -1, authrw.vertices().toArray()));
		tr.set("hub", g.collect(hub, -1, hubrw.vertices().toArray()));

		return before;
	}
}