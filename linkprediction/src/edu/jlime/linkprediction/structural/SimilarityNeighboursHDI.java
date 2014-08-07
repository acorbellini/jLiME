package edu.jlime.linkprediction.structural;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityNeighboursHDI implements ForEachQueryProc<Float> {

	private static final long serialVersionUID = -4604684934414135080L;

	private ListQuery query;

	public SimilarityNeighboursHDI(ListQuery query) {
		this.query = query;
	}

	@Override
	public Float call(ListQuery userId) throws Exception {
		int[] neighbours = query.query();
		int[] otherNeighbours = userId.neighbours().query();
		int intersect = IntArrayUtils.intersectCount(neighbours,
				otherNeighbours);
		double root = Math.max(otherNeighbours.length, neighbours.length);
		return intersect / (float) root;
	}
}
