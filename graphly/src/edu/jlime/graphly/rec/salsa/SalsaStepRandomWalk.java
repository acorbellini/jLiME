package edu.jlime.graphly.rec.salsa;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.MinEdgeFilter;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.set.hash.TLongHashSet;

public class SalsaStepRandomWalk implements CustomFunction {
	private String auth;
	private String hub;
	private int steps;
	private float max_depth;

	public SalsaStepRandomWalk(String auth, String hub, int steps, float max_depth2) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.max_depth = max_depth2;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		TLongHashSet res = before.vertices();

		Graph g = tr.getGraph();

		long[] authSet = g.v(res).filter(new MinEdgeFilter(Dir.IN, 1, res)).exec().vertices().toArray();
		g.v(authSet).set("mapper", tr.get("mapper")).as(Recommendation.class)
				.randomwalk(auth, steps, max_depth, authSet, Dir.IN, Dir.OUT).asTraversal()
				.join(auth, auth, new SalsaJoin()).exec();

		long[] hubSet = g.v(res).filter(new MinEdgeFilter(Dir.OUT, 1, res)).exec().vertices().toArray();
		g.v(hubSet).set("mapper", tr.get("mapper")).as(Recommendation.class)
				.randomwalk(hub, steps, max_depth, hubSet, Dir.OUT, Dir.IN).asTraversal()
				.join(hub, hub, new SalsaJoin()).exec();

		// tr.set("auth", g.collect(auth, -1, authrw.vertices().toArray()));
		// tr.set("hub", g.collect(hub, -1, hubrw.vertices().toArray()));

		return before;
	}
}