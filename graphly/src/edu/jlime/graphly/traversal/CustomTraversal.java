package edu.jlime.graphly.traversal;

import edu.jlime.jd.ClientNode;

public class CustomTraversal {
	protected GraphlyTraversal tr;

	public CustomTraversal(GraphlyTraversal tr) {
		this.tr = tr;
	}

	public GraphlyTraversal asTraversal() {
		return tr;
	}

	public Object exec() throws Exception {
		return tr.exec();
	}

	public Object submit(ClientNode c) throws Exception {
		return tr.submit(c);
	}
}
