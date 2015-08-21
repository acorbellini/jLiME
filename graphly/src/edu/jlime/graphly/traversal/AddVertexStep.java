package edu.jlime.graphly.traversal;

import gnu.trove.set.hash.TLongHashSet;

public class AddVertexStep implements Step {

	private int v;

	public AddVertexStep(int i, GraphlyTraversal graphlyTraversal) {
		this.v = i;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TLongHashSet res = before.vertices();
		res.add(v);
		return new VertexResult(res);
	}

}
