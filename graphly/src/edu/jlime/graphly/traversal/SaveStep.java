package edu.jlime.graphly.traversal;

/**
 * @author Alejandro
 *
 */
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

	@Override
	public String toString() {
		return "SaveStep [k=" + k + "]";
	}

}
