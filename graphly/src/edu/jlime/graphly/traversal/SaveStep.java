package edu.jlime.graphly.traversal;

public class SaveStep implements Step<Object, Object> {

	private String k;
	private GraphlyTraversal g;

	public SaveStep(String k, GraphlyTraversal g) {
		this.k = k;
		this.g = g;
	}

	@Override
	public Object exec(Object before) throws Exception {
		g.set(k, before);
		return before;
	}

}
