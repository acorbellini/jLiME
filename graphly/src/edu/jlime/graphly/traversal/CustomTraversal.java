package edu.jlime.graphly.traversal;

import edu.jlime.jd.Node;

public class CustomTraversal {
	protected Traversal tr;

	public CustomTraversal(Traversal tr) {
		this.tr = tr;
	}

	public Traversal asTraversal() {
		return tr;
	}

	public <T extends CustomTraversal> T as(Class<T> c) throws Exception {
		return tr.as(c);
	}

	public TraversalResult exec() throws Exception {
		return tr.exec();
	}

	public TraversalResult submit(Node c) throws Exception {
		return tr.submit(c);
	}
}
