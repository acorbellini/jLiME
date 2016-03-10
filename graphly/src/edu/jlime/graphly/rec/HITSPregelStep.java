package edu.jlime.graphly.rec;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.hits.HITSPregel;
import edu.jlime.graphly.rec.salsa.AuthHubResult;
import edu.jlime.graphly.traversal.PregelTraversal;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.util.MessageAggregators;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.mergers.MessageMergers;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class HITSPregelStep implements CustomFunction {

	private String auth;
	private String hub;
	private int steps;
	private int top;

	public HITSPregelStep(String auth, String hub, int steps, int top) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.top = top;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		TLongHashSet sg = before.vertices();

		Logger log = Logger.getLogger(HITSPregel.class);
		
		PregelConfig config = PregelConfig.create().aggregator("hits-auth", MessageAggregators.floatSum())
				.aggregator("hits-hub", MessageAggregators.floatSum()).merger("hits-auth", MessageMergers.floatSum())
				.merger("hits-hub", MessageMergers.floatSum()).steps(steps).subgraph("hits-sg", sg);

		Graph g = tr.getGraph();
		
		g.v(sg).set("mapper", tr.get("mapper")).as(PregelTraversal.class)
				.vertexFunction(new HITSPregel(auth, hub), config).exec();

		System.out.println(g.sumFloat(auth));
		System.out.println(g.sumFloat(hub));

		log.info("Counting top " + top);
		Set<Pair<Long, Float>> set = g.topFloat(auth, top);

		TLongFloatHashMap authRes = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : set) {
			authRes.put(pair.left, pair.right);
		}

		Set<Pair<Long, Float>> setHub = g.topFloat(hub, top);

		TLongFloatHashMap hubRes = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : setHub) {
			hubRes.put(pair.left, pair.right);
		}
		return new AuthHubResult(authRes, hubRes);
	}

}
