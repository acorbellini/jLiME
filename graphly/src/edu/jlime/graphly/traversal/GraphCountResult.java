package edu.jlime.graphly.traversal;

import java.util.Set;

import org.apache.log4j.Logger;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.util.Pair;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class GraphCountResult extends TraversalResult {

	private GraphlyGraph g;
	private String countK;
	private TLongHashSet vertices;

	public GraphCountResult(TLongHashSet vertices, GraphlyGraph graphlyGraph,
			String countK) {
		this.vertices = vertices;
		this.g = graphlyGraph;
		this.countK = countK;
	}

	@Override
	public float getCount(long key) throws Exception {
		return g.getFloat(key, countK);
	}

	@Override
	public TLongFloatHashMap getCounts() throws Exception {
		TLongFloatHashMap ret = g.getFloats(countK, vertices());
		return ret;
	}

	@Override
	public TraversalResult top(int top) throws Exception {
		Logger log = Logger.getLogger(GraphCountResult.class);
		log.info("Obtaining top " + top + " vertices from graph property "
				+ countK);
		Set<Pair<Long, Float>> t = g.topFloat(countK, top, vertices());
		TLongFloatHashMap ret = new TLongFloatHashMap();
		for (Pair<Long, Float> pair : t) {
			ret.put(pair.left, pair.right);
		}
		return new CountResult(ret);
	}

	@Override
	public TLongHashSet vertices() {
		return vertices;
	}

	@Override
	public TraversalResult removeAll(TLongHashSet v) {
		// TLongHashSet rem = new TLongHashSet(vertices);
		TLongIterator it = v.iterator();
		while (it.hasNext()) {
			vertices.remove(it.next());
		}
		return new GraphCountResult(vertices, g, countK);
	}

	@Override
	public TraversalResult retainAll(TLongHashSet v) {
		TLongHashSet ret = new TLongHashSet(vertices);
		ret.retainAll(v);
		return new GraphCountResult(ret, g, countK);
	}

}
