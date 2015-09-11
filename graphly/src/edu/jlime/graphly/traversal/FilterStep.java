package edu.jlime.graphly.traversal;

import gnu.trove.set.hash.TLongHashSet;

public class FilterStep implements Step {

	private TLongHashSet filter;
	protected Traversal g;
	private boolean neg;

	public FilterStep(long[] filter, Traversal graphlyTraversal) {
		this(filter, graphlyTraversal, false);
	}

	public FilterStep(long[] filter, Traversal graphlyTraversal, boolean b) {
		this.filter = new TLongHashSet(filter);
		this.g = graphlyTraversal;
		this.neg = b;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		return filter(before, filter);
	}

	protected TraversalResult filter(TraversalResult before, TLongHashSet filter) {
		if (neg)
			before.removeAll(filter);
		else
			before.retainAll(filter);
		return before;
	}
}
