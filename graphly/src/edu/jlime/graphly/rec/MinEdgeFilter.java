package edu.jlime.graphly.rec;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.client.Graphly;

public class MinEdgeFilter implements VertexFilter {

	private Dir dir;
	private int min;
	private long[] at;

	public MinEdgeFilter(Dir dir, int min, long[] amongThese) {
		this.dir = dir;
		this.min = min;
		this.at = amongThese;
		// Arrays.sort(at);
	}

	@Override
	public boolean filter(long vid, Graphly g) throws Exception {
		return g.getEdgesCount(dir, vid, at) >= min;
	}

}
