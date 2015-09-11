package edu.jlime.graphly.traversal;

public class MarkStep implements Step {

	private Traversal tr;
	private String k;

	public MarkStep(String k, Traversal graphlyTraversal) {
		this.k = k;
		this.tr = graphlyTraversal;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		tr.getGraph().setProperty(before.vertices(), "mark", k);
		return before;
	}

}
