package edu.jlime.graphly.traversal;

import gnu.trove.set.hash.TLongHashSet;

public class AddVertexStep implements Step {

	private long[] v;

	public AddVertexStep(Traversal graphlyTraversal, long... users) {
		this.v = users;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TLongHashSet res = before.vertices();
		res.addAll(v);
		return new VertexResult(res);
	}

}
