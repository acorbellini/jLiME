package edu.jlime.graphly.recommendation;

import java.util.Arrays;
import java.util.Map;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.recommendation.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CustomTraversal;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;

public class Recommendation extends CustomTraversal {

	private final class HITSCustomStep implements CustomFunction {
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

	private static class SalsaStep implements CustomFunction {
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
		public TraversalResult execute(TraversalResult before,
				GraphlyTraversal tr) throws Exception {
			long[] res = before.vertices().toArray();

			Graphly g = tr.getGraph();

			TraversalResult authrw = g
					.v(res)
					.set("mapper", tr.get("mapper"))
					.filter(new MinEdgeFilter(Dir.IN, 1, res))
					.as(Recommendation.class)
					.randomwalk(auth, steps, max_depth, res, Dir.BOTH, Dir.BOTH)
					.asTraversal().join(auth, auth, new SalsaJoin()).exec();

			TraversalResult hubrw = g.v(res).set("mapper", tr.get("mapper"))
					.filter(new MinEdgeFilter(Dir.OUT, 1, res))
					.as(Recommendation.class)
					.randomwalk(hub, steps, max_depth, res, Dir.BOTH, Dir.BOTH)
					.asTraversal().join(hub, hub, new SalsaJoin()).exec();

			tr.set("auth", g.collect(auth, authrw.vertices().toArray()));
			tr.set("hub", g.collect(hub, hubrw.vertices().toArray()));

			return before;
		}
	}

	public Recommendation(GraphlyTraversal tr) {
		super(tr);
	}

	public Recommendation exploratoryCount(Dir first, Dir... dirs)
			throws Exception {
		tr.save("target")
				.to(first)
				.save("first")
				.traverse(new String[] { "first", "target" },
						Arrays.copyOf(dirs, dirs.length - 1))
				.count(dirs[dirs.length - 1]).filterBy("first", "target");
		return this;
	}

	public Recommendation randomwalk(String key, int steps, int max_depth,
			long[] subset, Dir... dirs) {
		tr.each(steps, key, new RandomWalkForeach(max_depth, subset, dirs));
		tr.join(key, key, new RandomWalkJoin());
		return this;
	}

	public Recommendation randomwalk(String key, int steps, int max_depth,
			Dir... dirs) {
		return randomwalk(key, steps, max_depth, new long[] {}, dirs);
	}

	public Recommendation hits(String auth, String hub, int steps, int max_edges)
			throws Exception {
		tr.traverse(max_edges, Dir.BOTH);
		tr.customStep(new HITSCustomStep(auth, hub, steps, max_edges));
		return this;
	}

	public Recommendation salsa(String auth, String hub, int steps,
			int max_depth) throws Exception {
		tr.customStep(new SalsaStep(auth, hub, steps, max_depth));
		return this;
	}
}
