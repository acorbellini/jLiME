package edu.jlime.graphly.rec;

import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.hits.HITSPregel;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.Pregel;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.graphly.util.MessageAggregators;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.mergers.MessageMergers;
import gnu.trove.set.hash.TLongHashSet;

public class HITSPregelStep implements CustomFunction {

	private String auth;
	private String hub;
	private int steps;

	public HITSPregelStep(String auth, String hub, int steps) {
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		TLongHashSet sg = before.vertices();

		PregelConfig config = PregelConfig.create()
				.aggregator("hits-auth", MessageAggregators.floatSum())
				.aggregator("hits-hub", MessageAggregators.floatSum())
				.merger("hits-auth", MessageMergers.floatSum())
				.merger("hits-hub", MessageMergers.floatSum()).steps(steps)
				.subgraph("hits-sg", sg);

		tr.getGraph().v(sg).set("mapper", MapperFactory.location())
				.as(Pregel.class)
				.vertexFunction(new HITSPregel(auth, hub), config).exec();
		return before;
	}

}
