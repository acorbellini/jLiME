package edu.jlime.linkprediction.structural;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityNeighboursHPI implements ForEachQueryProc<Float> {

	private static final long serialVersionUID = -3372672019199255097L;

	private ListQuery query;

	public SimilarityNeighboursHPI(ListQuery query) {
		this.query = query;
	}

	@Override
	public Float call(ListQuery userId) throws Exception {
		int[] neighbours = query.query();
		int[] otherNeighbours = userId.neighbours().query();
		int intersect = IntArrayUtils.intersectCount(neighbours,
				otherNeighbours);
		double root = Math.min(otherNeighbours.length, neighbours.length);
		return intersect / (float) root;
	}

}
