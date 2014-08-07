package edu.jlime.linkprediction.structural;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityNeighboursLHNI implements ForEachQueryProc<Float> {

	private static final long serialVersionUID = 8783454177105636595L;

	private ListQuery query;

	public SimilarityNeighboursLHNI(ListQuery query) {
		this.query = query;
	}

	@Override
	public Float call(ListQuery userId) throws Exception {
		int[] neighbours = query.query();
		int[] otherNeighbours = userId.neighbours().query();
		int intersect = IntArrayUtils.intersectCount(neighbours,
				otherNeighbours);
		float root = otherNeighbours.length * neighbours.length;
		return intersect / root;
	}

}
