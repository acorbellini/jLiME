package edu.jlime.graphly.traversal;

public class VarFilterStep extends FilterStep {

	private String[] k;

	public VarFilterStep(String[] k, GraphlyTraversal g) {
		super(null, g);
		this.k = k;
	}

	@Override
	public Object exec(Object before) throws Exception {
		Object res = before;
		for (String k : k) {
			res = super.filter(res, (long[]) super.g.get(k));
		}
		return res;
	}

}
