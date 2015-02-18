package edu.jlime.graphly.traversal;

public class TopStep implements Step {

	private int top;
	private GraphlyTraversal tr;

	public TopStep(int top, GraphlyTraversal tr) {
		this.top = top;
		this.tr = tr;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		return before.top(top);
	}

}
