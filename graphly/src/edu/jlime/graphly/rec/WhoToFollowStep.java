package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

public class WhoToFollowStep implements CustomFunction {

	private String auth;
	private String hub;
	private int steps;
	private float max_depth;

	public WhoToFollowStep(String a, String h, int steps, float maxsalsadepth, int circleTop) {
		this.auth = a;
		this.hub = h;
		this.steps = steps;
		this.max_depth = maxsalsadepth;

	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		long[] target = before.vertices().toArray();
		Graph g = tr.getGraph();
		g.v(target).set("mapper", tr.get("mapper")).as(Recommendation.class).salsaHybrid(auth, hub, steps, 10).exec();
		TLongObjectHashMap<Object> collected = g.collect(auth, 100, target);

		TLongFloatHashMap ret = new TLongFloatHashMap();

		TLongObjectIterator<Object> it = collected.iterator();
		while (it.hasNext()) {
			it.advance();
			ret.put(it.key(), (Float) it.value());
		}
		return new CountResult(ret);
	}

}
