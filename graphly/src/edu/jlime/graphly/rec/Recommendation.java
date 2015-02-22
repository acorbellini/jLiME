package edu.jlime.graphly.rec;

import java.util.Arrays;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.randomwalk.RandomWalkForeach;
import edu.jlime.graphly.rec.randomwalk.RandomWalkJoin;
import edu.jlime.graphly.rec.salsa.SalsaStep;
import edu.jlime.graphly.rec.salsa.SalsaStepRandomWalk;
import edu.jlime.graphly.traversal.CustomTraversal;
import edu.jlime.graphly.traversal.Dir;
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

	public Recommendation(GraphlyTraversal tr) {
		super(tr);
	}

	public Recommendation exploratoryCount(int max_edges, int top, Dir... dirs)
			throws Exception {
		tr.save("target")
				.to(dirs[0], max_edges)
				.save("first")
				.traverse(new String[] { "first", "target" }, max_edges,
						Arrays.copyOfRange(dirs, 1, dirs.length - 1))
				.count(dirs[dirs.length - 1]).filterBy("first", "target")
				.top(top);
		return this;
	}

	public Recommendation randomwalk(String key, int steps, float max,
			long[] subset, Dir... out) {
		tr.each(steps, key, new RandomWalkForeach(max, subset, out));
		tr.join(key, key, new RandomWalkJoin());
		return this;
	}

	public Recommendation randomwalk(String key, int steps, float max,
			Dir... out) {
		return randomwalk(key, steps, max, new long[] {}, out);
	}

	public Recommendation hits(String auth, String hub, int steps, int max_edges)
			throws Exception {
		tr.traverse(max_edges, Dir.BOTH);
		tr.customStep(new HITSCustomStep(auth, hub, steps, max_edges));
		return this;
	}

	public Recommendation salsa(String auth, String hub, int steps,
			float max_depth) throws Exception {
		tr.customStep(new SalsaStep(auth, hub, steps, max_depth));
		return this;
	}

	public Recommendation salsaRW(String auth, String hub, int steps,
			float max_depth) throws Exception {
		tr.customStep(new SalsaStepRandomWalk(auth, hub, steps, max_depth));
		return this;
	}

	public Recommendation whotofollow(String a, String h, int steps,
			float max_depth, int topCircle, int salsasteps,
			float maxsalsadepth, int topAuth) {
		circleOfTrust(steps, max_depth, topCircle).asTraversal().customStep(
				new WhoToFollowStep(a, h, salsasteps, maxsalsadepth, topAuth));
		return this;
	}

	private Recommendation circleOfTrust(int steps, float max_depth, int top) {
		tr.customStep(new CircleOfTrust(steps, max_depth, top));
		return this;
	}

	public Recommendation commonNeighbours() {
		
		return this;
	}
}
