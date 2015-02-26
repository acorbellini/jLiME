package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.map.hash.TLongObjectHashMap;

public class WhoToFollowStep implements CustomFunction {

	private String auth;
	private String hub;
	private int steps;
	private float max_depth;

	public WhoToFollowStep(String a, String h, int steps, float maxsalsadepth,
			int circleTop) {
		this.auth = a;
		this.hub = h;
		this.steps = steps;
		this.max_depth = maxsalsadepth;

	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		long[] target = before.vertices().toArray();
		Graphly g = tr.getGraph();
		g.v(target).set("mapper", tr.get("mapper")).as(Recommendation.class)
				.salsa(auth, hub, steps).exec();
		TLongObjectHashMap<Object> res = g.collect(auth, 100, target);
		return new CountResult(res);
	}

}
