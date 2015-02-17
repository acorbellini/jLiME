package edu.jlime.graphly.traversal;

public class SaveStep implements Step {

	private String k;
	private GraphlyTraversal g;

	public SaveStep(String k, GraphlyTraversal g) {
		this.k = k;
		this.g = g;
	}

	@Override
	public TraversalResult exec(TraversalResult before) throws Exception {
		g.set(k, before);
		return before;
	}

}
