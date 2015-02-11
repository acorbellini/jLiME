package edu.jlime.graphly.blueprints;

import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.util.DefaultTraversal;

public class GraphlyGraphTraversal<S, E> extends DefaultTraversal<S, E>
		implements GraphTraversal<S, E>, GraphTraversal.Admin<S, E> {

	public GraphlyGraphTraversal(Class<?> emanatingClass) {
		super(emanatingClass);
	}

	@Override
	public GraphTraversal.Admin<S, E> asAdmin() {
		return this;
	}
}
