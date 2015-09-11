package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.traversal.Dir;
import gnu.trove.set.hash.TLongHashSet;

public class MinEdgeFilter implements VertexFilter {

	private Dir dir;
	private int min;
	private TLongHashSet at;

	public MinEdgeFilter(Dir dir, int min, TLongHashSet amongThese) {
		this.dir = dir;
		this.min = min;
		this.at = amongThese;
		// Arrays.sort(at);
	}

	@Override
	public boolean filter(long vid, Graph g) throws Exception {
		return g.getEdgesCount(dir, vid, at) >= min;
	}

}
