package edu.jlime.graphly.rec;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.salsa.AuthHubResult;
import edu.jlime.graphly.rec.salsa.SALSAPregel;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.Pregel;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.mergers.MessageMergers;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class SALSAPregelStep implements CustomFunction {

	private String auth;
	private String hub;
	private int steps;
	private int top;

	public SALSAPregelStep(String auth, String hub, int steps, int top) {
		super();
		this.auth = auth;
		this.hub = hub;
		this.steps = steps;
		this.top = top;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		Logger log = Logger.getLogger(SALSAPregelStep.class);

		TLongHashSet subgraph = before.vertices();

		log.info("Executing Salsa Step on " + subgraph.size());

		Graph g = tr.getGraph();

		log.info("Filtering authority side");
		TLongHashSet authSet = g.v(subgraph.toArray()).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.IN, 1, subgraph)).exec().vertices();

		log.info("Filtering hub side");
		TLongHashSet hubSet = g.v(subgraph.toArray()).set("mapper", tr.get("mapper"))
				.filter(new MinEdgeFilter(Dir.OUT, 1, subgraph)).exec().vertices();
		log.info("Executing SalsaPregel with hubset " + hubSet.size() + " and auth " + authSet.size());

		PregelConfig config = PregelConfig.create().merger("salsa-auth", MessageMergers.floatSum())
				.merger("salsa-hub", MessageMergers.floatSum()).steps(steps).subgraph("salsa-sg", subgraph);

		g.v(subgraph).set("mapper", tr.get("mapper")).as(Pregel.class)
				.vertexFunction(new SALSAPregel(auth, hub, authSet.size(), hubSet.size()), config).exec();

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
