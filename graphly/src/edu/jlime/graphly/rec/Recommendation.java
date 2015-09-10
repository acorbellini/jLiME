package edu.jlime.graphly.rec;

import java.util.Arrays;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.hits.HITSHybridStep;
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

	public Recommendation exploratoryCountHybrid(int max_edges, int top,
			String countK, Dir... dirs) throws Exception {

		tr.customStep(new ExploratoryCountHybrid(max_edges, top, countK, dirs));

		return this;
	}

	public Recommendation exploratoryCount(int max_edges, int top, Dir... dirs)
			throws Exception {

		tr.save("target").to(dirs[0], max_edges)
				.save("first").traverseCount(new String[] { "first", "target" },
						max_edges, Arrays.copyOfRange(dirs, 1, dirs.length))
				.top(top);

		return this;
	}

	public Recommendation exploratoryCountPregel(int top) throws Exception {
		tr.customStep(new ExploratoryCountPregel(top));
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

	public Recommendation hits(int steps, int top) throws Exception {
		tr.customStep(new HITSStep(steps)).top(top);
		return this;
	}

	public Recommendation salsa(int steps, int top) throws Exception {
		tr.customStep(new SalsaStep(steps * 2)).top(top);
		return this;
	}

	public Recommendation salsaRW(String auth, String hub, int steps,
			float max_depth) throws Exception {
		tr.customStep(new SalsaStepRandomWalk(auth, hub, steps, max_depth));
		return this;
	}

	public Recommendation whotofollow(String a, String h, int steps,
			float max_depth, int topCircle, int salsasteps, float maxsalsadepth,
			int topAuth) {
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
				.haltCondition(new PageRankHaltCondition(cut, "pr"))
				.executeOnAll(true);

		tr.as(Pregel.class).vertexFunction(
				new PageRankFloat(pagerankProp, vertexCount), config);

		return this;
	}

	public Recommendation hitsPregel(String auth, String hub, int steps,
			int top) throws Exception {

		tr.customStep(new HITSPregelStep(auth, hub, steps + 1, top));

		return this;
	}

	public Recommendation salsaPregel(String auth, String hub, int steps,
			int top) throws Exception {

		tr.customStep(new SALSAPregelStep(auth, hub, 2 * steps, top));

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

	public CustomTraversal katz(String val, int steps, float beta, Dir dir)
			throws Exception {
		// plus 1 to activate origin
		PregelConfig config = PregelConfig.create().steps(steps + 1)
				.merger("katz", MessageMergers.floatSum());

		tr.as(Pregel.class).vertexFunction(
				new KatzRootedPregel(val, beta, steps, dir), config);

		return this;
	}

	public CustomTraversal katzFJ(float beta, int depth, int top, Dir dir) {
		tr.customStep(new BetaCountStep(new KatzBeta(beta), depth, dir))
				.top(top);
		return this;
	}

	public CustomTraversal commonNeighboursPregel() throws Exception {

		int res = tr.exec().vertices().size();

		PregelConfig config = PregelConfig.create().steps(1).aggregator("cm",
				new IntersectAggregator());

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

		PregelConfig config = PregelConfig.create().steps(1).aggregator("cm",
				new JaccardAggregator());

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

	public CustomTraversal localPathFJ(float alpha, int top, Dir dir) {

		tr.customStep(new BetaCountStep(new LPBeta(alpha), 3, dir)).top(top);

		return this;
	}

	public CustomTraversal localPathPregel(String key, Dir dir, float alpha)
			throws Exception {
		PregelConfig config = PregelConfig.create().steps(4)// +1 para poder
															// guardar los datos
				.merger("lp", MessageMergers.floatSum());

		tr.as(Pregel.class).vertexFunction(new LocalPath(key, alpha, dir),
				config);

		return this;
	}

	public CustomTraversal friendLinkFJ(int top, int depth, Dir dir)
			throws Exception {
		long vertices = tr.getGraph().getVertexCount();

		tr.customStep(
				new BetaCountStep(new FriendLinkBeta(vertices), depth, dir))
				.top(top);

		return this;

	}

	public CustomTraversal friendLinkPregel(String k, int depth, Dir dir)
			throws Exception {
		long vertices = tr.getGraph().getVertexCount();
		PregelConfig config = PregelConfig.create().steps(depth + 1)// +1 para
				// poder
				// guardar los datos
				.merger("fl", MessageMergers.floatSum());

		tr.as(Pregel.class).vertexFunction(
				new FriendLink(k, vertices, depth, dir), config);

		return this;
	}

	public CustomTraversal friendLinkHybrid(int top, int depth, Dir dir)
			throws Exception {
		long vertices = tr.getGraph().getVertexCount();

		Dir[] dirs = new Dir[depth];
		Arrays.fill(dirs, dir);
		tr.customStep(new BetaCountHybrid(new FriendLinkBeta(vertices), top,
				"fl", dirs));

		return this;
	}

	public CustomTraversal katzHybrid(float beta, int depth, int top, Dir dir) {

		Dir[] dirs = new Dir[depth];
		Arrays.fill(dirs, dir);

		tr.customStep(
				new BetaCountHybrid(new KatzBeta(beta), top, "katz", dirs));
		return this;
	}

	public CustomTraversal localPathHybrid(float alpha, int top, Dir dir) {
		Dir[] dirs = new Dir[3];
		Arrays.fill(dirs, dir);
		tr.customStep(new BetaCountHybrid(new LPBeta(alpha), top, "lp", dirs));
		return this;
	}

	public CustomTraversal salsaHybrid(String auth, String hub, int steps,
			int top) {
		tr.customStep(new SalsaHybrid(auth, hub, steps * 2, top));
		return this;
	}

	public CustomTraversal hitsHybrid(String auth, String hub, int steps,
			int top) {
		tr.customStep(new HITSHybridStep(auth, hub, steps, top));
		return this;
	}
}
