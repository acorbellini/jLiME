package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.traversal.Dir;

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
	public boolean filter(long vid, GraphlyGraph g) throws Exception {
		return g.getEdgesCount(dir, vid, at) >= min;
	}

}
