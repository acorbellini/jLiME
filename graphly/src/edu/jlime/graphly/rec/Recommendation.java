package edu.jlime.graphly.rec;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.hits.HITSStep;
import edu.jlime.graphly.rec.randomwalk.RandomWalkForeach;
import edu.jlime.graphly.rec.randomwalk.RandomWalkJoin;
import edu.jlime.graphly.rec.salsa.SalsaStep;
import edu.jlime.graphly.rec.salsa.SalsaStepRandomWalk;
import edu.jlime.graphly.traversal.CustomTraversal;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Pregel;
import edu.jlime.graphly.util.FloatSumAggregator;
import edu.jlime.graphly.util.MessageAggregators;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.functions.PageRankFloat;
import edu.jlime.pregel.functions.PageRankFloat.PageRankHaltCondition;
import edu.jlime.pregel.mergers.MessageMergers;

public class Recommendation extends CustomTraversal {

	Logger log = Logger.getLogger(Recommendation.class);

	public Recommendation(GraphlyTraversal tr) {
		super(tr);
	}

	public Recommendation exploratoryCount(int max_edges, int top,
			String countK, Dir... dirs) throws Exception {
		// tr.customStep(new ExploratoryCountStep(max_edges, dirs));

		tr.save("target")
				.to(dirs[0], max_edges)
				.save("first")
				.traverseGraphCount(countK, new String[] { "first", "target" },
						max_edges, Arrays.copyOfRange(dirs, 1, dirs.length))
				.top(top);

		return this;
	}

	public Recommendation exploratoryCountPregel() throws Exception {

		PregelConfig config = PregelConfig.create()
				.merger("ec", MessageMergers.floatSum()).steps(4);

		tr.as(Pregel.class).vertexFunction(
				new ExploratoryCountVertexFunction(), config);

		// Set<Pair<Long, Float>> top_count = tr.getGraph().topFloat("count",
		// 10);

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

	public Recommendation hits(String auth, String hub, int steps)
			throws Exception {
		tr.customStep(new HITSStep(auth, hub, steps));
		return this;
	}

	public Recommendation salsa(String auth, String hub, int steps)
			throws Exception {
		tr.customStep(new SalsaStep(auth, hub, steps * 3));
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

		PregelConfig config = PregelConfig.create().steps(steps)
				.aggregator("pr", MessageAggregators.floatSum())
				.merger("pr", MessageMergers.floatSum())
				.haltCondition(new PageRankHaltCondition(cut))
				.executeOnAll(true);

		tr.as(Pregel.class).vertexFunction(
				new PageRankFloat(pagerankProp, vertexCount), config);

		return this;
	}

	public Recommendation hitsPregel(String auth, String hub, int steps)
			throws Exception {

		tr.customStep(new HITSPregelStep(auth, hub, steps));

		return this;
	}

	public Recommendation salsaPregel(String auth, String hub, int steps)
			throws Exception {

		tr.customStep(new SALSAPregelStep(auth, hub, 3 * steps));

		return this;
	}

	// public Recommendation whotofollow(String auth, String hub, int steps)
	// throws Exception {
	// GraphlyGraph g = tr.getGraph();
	//
	// randomwalk("whotofollow-rw", steps, max, out);
	//
	// PregelConfig config = PregelConfig.create()
	// .merger("salsa", new SalsaMerger()).steps(steps)
	// .executeOnAll(true);
	//
	// tr.as(Pregel.class).vertexFunction(
	// new SALSAPregel(auth, hub, vertexCount), config);

	// return this;
	// }

	public CustomTraversal katz(String val, int steps, float beta)
			throws Exception {
		// plus 1 to activate origin
		PregelConfig config = PregelConfig.create().steps(steps + 1)
				.merger("katz", MessageMergers.floatSum());

		tr.as(Pregel.class).vertexFunction(new KatzPregel(val, beta, steps),
				config);

		return this;
	}

	public CustomTraversal katzFJ(float beta, int depth, int top) {
		tr.customStep(new BetaCountStep(new KatzBeta(beta), depth, Dir.OUT))
				.top(top);
		return this;
	}

	public CustomTraversal commonNeighboursPregel() throws Exception {

		int res = tr.exec().vertices().size();

		PregelConfig config = PregelConfig.create().steps(1)
				.aggregator("cm", new IntersectAggregator());

		tr.as(Pregel.class)
				.vertexFunction(new CommonNeighboursPregel(res), config)
				.aggregatorValue("cm");

		return this;
	}

	public CustomTraversal commonFJ() {
		tr.intersect(Dir.BOTH).size();
		return this;
	}

	public CustomTraversal jaccardFJ() {
		tr.customStep(new JaccardStep());
		return this;
	}

	public CustomTraversal jaccardPregel() throws Exception {
		int res = tr.exec().vertices().size();

		PregelConfig config = PregelConfig.create().steps(1)
				.aggregator("cm", new JaccardAggregator());

		tr.as(Pregel.class)
				.vertexFunction(new CommonNeighboursPregel(res), config)
				.aggregatorValue("cm");

		return this;
	}

	public CustomTraversal adamicFJ() {
		tr.intersect(Dir.BOTH).customStep(new AdamicAdar());
		return this;
	}

	public CustomTraversal adamicPregel() throws Exception {
		int res = tr.exec().vertices().size();

		PregelConfig config = PregelConfig.create().steps(1)
				.aggregator("cm", new IntersectAggregator())
				.aggregator("adamic", new FloatSumAggregator());

		tr.as(Pregel.class)
				.vertexFunction(new CommonNeighboursPregel(res), config)
				.aggregatorSet("cm")
				.vertexFunction(new AdamicAdarPregel(), config)
				.aggregatorValue("adamic");

		return this;
	}

	public CustomTraversal localPathFJ(float alpha, int top) {

		tr.customStep(new BetaCountStep(new LPBeta(alpha), 3, Dir.OUT))
				.top(top);

		return this;
	}

	public CustomTraversal localPathPregel(String key, float alpha)
			throws Exception {
		PregelConfig config = PregelConfig.create().steps(4)// +1 para poder
															// guardar los datos
				.merger("lp", MessageMergers.floatSum());

		tr.as(Pregel.class).vertexFunction(new LocalPath(key, alpha), config);

		return this;
	}

	public CustomTraversal friendLinkFJ(int top) throws Exception {
		long vertices = tr.getGraph().getVertexCount();

		tr.customStep(
				new BetaCountStep(new FriendLinkBeta(vertices), 3, Dir.OUT))
				.top(top);

		return this;

	}

	public CustomTraversal friendLinkPregel(String k, int depth)
			throws Exception {
		long vertices = tr.getGraph().getVertexCount();
		PregelConfig config = PregelConfig.create().steps(depth + 1)// +1 para
				// poder
				// guardar los datos
				.merger("fl", MessageMergers.floatSum());

		tr.as(Pregel.class).vertexFunction(new FriendLink(k, vertices, depth),
				config);

		return this;
	}
}
