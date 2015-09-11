package edu.jlime.graphly.rec;

import java.util.Arrays;
import java.util.Set;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class ExploratoryCountHybrid implements CustomFunction {

	private int max_edges;
	private int top;
	private String countK;
	private Dir[] dirs;

	public ExploratoryCountHybrid(int max_edges, int top, String countK, Dir... dirs) {
		this.max_edges = max_edges;
		this.top = top;
		this.countK = countK;
		this.dirs = dirs;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {

		Graph g = tr.getGraph();
		TLongHashSet vertices = before.vertices();

		TLongHashSet list = g.v(vertices).set("mapper", tr.get("mapper")).to(dirs[0], max_edges).exec().vertices();

		TLongHashSet filter = new TLongHashSet(list);
		filter.addAll(vertices);
		g.v(list).set("mapper", tr.get("mapper"))
				.traverseGraphCount(countK, null, filter, max_edges, null, Arrays.copyOfRange(dirs, 1, dirs.length))
				.exec();

		Set<Pair<Long, Float>> set = g.topFloat(countK, top);

		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : set) {
			ret.put(pair.left, pair.right);
		}
		return new CountResult(ret);
	}

}
