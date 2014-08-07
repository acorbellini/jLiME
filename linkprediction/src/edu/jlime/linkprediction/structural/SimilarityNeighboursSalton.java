package edu.jlime.linkprediction.structural;

import edu.jlime.collections.adjacencygraph.query.ForEachQueryProc;
import edu.jlime.collections.adjacencygraph.query.ListQuery;
import edu.jlime.collections.util.IntArrayUtils;

public class SimilarityNeighboursSalton implements ForEachQueryProc<Float> {

	private static final long serialVersionUID = 5723711762106306668L;

	private ListQuery query;

	public SimilarityNeighboursSalton(ListQuery query) {
		this.query = query;
	}

	@Override
	public Float call(ListQuery userId) throws Exception {
		int[] neighbours = query.query();
		int[] otherNeighbours = userId.neighbours().query();
		int intersect = IntArrayUtils.intersectCount(neighbours,
				otherNeighbours);
		double root = Math.sqrt(otherNeighbours.length * neighbours.length);
		return intersect / (float) root;
	}

}
