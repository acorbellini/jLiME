package edu.jlime.graphly.rec;

import java.util.Arrays;
import java.util.Set;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.graphly.traversal.GraphlyTraversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.util.Pair;
import gnu.trove.map.hash.TLongFloatHashMap;

public class ExploratoryCountHybrid implements CustomFunction {

	private int max_edges;
	private int top;
	private String countK;
	private Dir[] dirs;

	public ExploratoryCountHybrid(int max_edges, int top, String countK,
			Dir... dirs) {
		this.max_edges = max_edges;
		this.top = top;
		this.countK = countK;
		this.dirs = dirs;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {

		GraphlyGraph g = tr.getGraph();
		g.v(before.vertices())
				.set("mapper", tr.get("mapper"))
				.mark("target")
				.to(dirs[0], max_edges)
				.mark("first")
				.traverseGraphCount(countK, new String[] { "first", "target" },
						max_edges, Arrays.copyOfRange(dirs, 1, dirs.length))
				.exec();

		Set<Pair<Long, Float>> set = g.topFloat(countK, top);

		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : set) {
			ret.put(pair.left, pair.right);
		}
		return new CountResult(ret);
	}

}
