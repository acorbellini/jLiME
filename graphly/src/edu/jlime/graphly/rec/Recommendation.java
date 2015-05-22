package edu.jlime.graphly.rec;

import java.util.Arrays;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.hits.HITSCustomStep;
import edu.jlime.graphly.rec.hits.HITSPregel;
import edu.jlime.graphly.rec.hits.HITSPregel.HITSMerger;
import edu.jlime.graphly.rec.randomwalk.RandomWalkForeach;
import edu.jlime.graphly.rec.randomwalk.RandomWalkJoin;
import edu.jlime.graphly.rec.salsa.SALSAPregel;
import edu.jlime.graphly.rec.salsa.SALSAPregel.SalsaMerger;
import edu.jlime.graphly.rec.salsa.SalsaStep;
import edu.jlime.graphly.rec.salsa.SalsaStepRandomWalk;
import edu.jlime.graphly.traversal.CustomTraversal;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Pregel;
import edu.jlime.graphly.util.MessageAggregators;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.coordinator.CoordinatorTask;
import edu.jlime.pregel.coordinator.HaltCondition;
import edu.jlime.pregel.functions.PageRankFloat;
import edu.jlime.pregel.mergers.MessageMergers;
import edu.jlime.pregel.worker.FloatAggregator;

public class Recommendation extends CustomTraversal {

	public static final class PageRankHaltCondition implements HaltCondition {
		private float cut;

		public PageRankHaltCondition(float cut) {
			this.cut = cut;
		}

		@Override
		public boolean eval(CoordinatorTask coordinatorTask, int step) {
			if (step <= 1)
				return false;

			FloatAggregator ag = (FloatAggregator) coordinatorTask
					.getAggregator("pr");
			float f = ag.get();
			// System.out.printf("Current Difference: %.20f \n", f);
			return f < cut;
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
				.count(dirs[dirs.length - 1], top, max_edges)
				.filterBy("first", "target");
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
		tr.to(Dir.BOTH, max_edges).customStep(
				new HITSCustomStep(auth, hub, steps, max_edges));
		return this;
	}

	public Recommendation salsa(String auth, String hub, int steps)
			throws Exception {
		tr.customStep(new SalsaStep(auth, hub, steps));
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

	public Recommendation pagerank(String pagerankProp, int steps, float cut)
			throws Exception {
		GraphlyGraph g = tr.getGraph();
		int vertexCount = g.getVertexCount();

		g.setDefaultFloat(pagerankProp, 1f / vertexCount);
		g.setDefaultFloat("ranksource", .85f);

		PregelConfig config = PregelConfig.create()
				.aggregator("pr", MessageAggregators.FLOAT_SUM)
				.haltCondition(new PageRankHaltCondition(cut)).steps(steps)
				.executeOnAll(true).merger("pr", MessageMergers.FLOAT_SUM);

		tr.as(Pregel.class).vertexFunction(
				new PageRankFloat(pagerankProp, vertexCount), config);

		return this;
	}

	public Recommendation hitsPregel(String auth, String hub, int steps)
			throws Exception {
		GraphlyGraph g = tr.getGraph();
		int vertexCount = g.getVertexCount();

		g.setDefaultFloat(auth, (float) (1f / Math.sqrt(vertexCount)));
		g.setDefaultFloat(hub, (float) (1f / Math.sqrt(vertexCount)));

		PregelConfig config = PregelConfig.create()
				.merger("hits", new HITSMerger()).steps(steps)
				.executeOnAll(true);

		tr.as(Pregel.class).vertexFunction(
				new HITSPregel(auth, hub, vertexCount), config);

		return this;
	}

	public Recommendation salsaPregel(String auth, String hub, int steps)
			throws Exception {
		GraphlyGraph g = tr.getGraph();
		int vertexCount = g.getVertexCount();

		float init = 1f / vertexCount;
		g.setDefaultFloat(auth, init);
		g.setDefaultFloat(hub, init);

		PregelConfig config = PregelConfig.create()
				.merger("salsa", new SalsaMerger()).steps(steps)
				.executeOnAll(true);

		tr.as(Pregel.class).vertexFunction(
				new SALSAPregel(auth, hub, vertexCount), config);

		return this;
	}

}
