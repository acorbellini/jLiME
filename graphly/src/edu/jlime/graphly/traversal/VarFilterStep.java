package edu.jlime.graphly.traversal;

import gnu.trove.list.array.TLongArrayList;


public class VarFilterStep implements Step {

	private String[] kList;
	private GraphlyTraversal g;
	private boolean neg;

	public VarFilterStep(String[] k, GraphlyTraversal g, boolean neg) {
		this.kList = k;
		this.g = g;
		this.neg = neg;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		TraversalResult res = before;
		for (String k : kList) {
			TLongArrayList filter = ((TraversalResult) g.get(k)).vertices();

			if (neg)
				res = res.removeAll(filter);
			else
				res = res.retainAll(filter);
		}
		return res;
	}

}
