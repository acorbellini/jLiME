package edu.jlime.graphly.rec;

import java.util.Arrays;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class BetaCountHybrid implements CustomFunction {

	private int max_edges;
	private int top;
	private String countK;
	private Dir[] dirs;
	private BetaCalc calc;

	public BetaCountHybrid(BetaCalc calc, int top, String countK, Dir... dirs) {
		this.max_edges = Integer.MAX_VALUE;
		this.top = top;
		this.countK = countK;
		this.dirs = dirs;
		this.calc = calc;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr) throws Exception {

		GraphlyGraph g = tr.getGraph();
		TLongHashSet vertices = before.vertices();

		g.v(vertices).set("mapper", tr.get("mapper"))
				.traverseGraphCount("beta-count", countK, null, max_edges, calc, dirs).exec();
		Logger log = Logger.getLogger(BetaCountHybrid.class);

		log.info("Counting top " + top);
		Set<Pair<Long, Float>> set = g.topFloat(countK, top);

		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : set) {
			ret.put(pair.left, pair.right);
		}
		return new CountResult(ret);
	}

}
