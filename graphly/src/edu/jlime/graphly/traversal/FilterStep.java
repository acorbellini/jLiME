package edu.jlime.graphly.traversal;

import gnu.trove.list.array.TLongArrayList;

public class FilterStep implements Step {

	private TLongArrayList filter;
	protected GraphlyTraversal g;
	private boolean neg;

	public FilterStep(long[] filter, GraphlyTraversal graphlyTraversal) {
		this(filter, graphlyTraversal, false);
	}

	public FilterStep(long[] filter, GraphlyTraversal graphlyTraversal,
			boolean b) {
		this.filter = TLongArrayList.wrap(filter);
		this.g = graphlyTraversal;
		this.neg = b;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		return filter(before, filter);
	}

	protected TraversalResult filter(TraversalResult before,
			TLongArrayList filter) {
		if (neg)
			before.removeAll(filter);
		else
			before.retainAll(filter);
		return before;
	}
}
