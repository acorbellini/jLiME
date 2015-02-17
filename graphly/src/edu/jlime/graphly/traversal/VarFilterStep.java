package edu.jlime.graphly.traversal;

import gnu.trove.list.array.TLongArrayList;

public class VarFilterStep implements Step {

	private String[] k;
	private GraphlyTraversal g;
	private boolean neg;

	public VarFilterStep(String[] k, GraphlyTraversal g, boolean neg) {
		this.k = k;
		this.g = g;
		this.neg = neg;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TraversalResult res = before;
		for (String k : k) {
			TLongArrayList filter = ((TraversalResult) g.get(k)).vertices();

			if (neg)
				res = res.removeAll(filter);
			else
				res = res.retainAll(filter);
		}
		return res;
	}

}
