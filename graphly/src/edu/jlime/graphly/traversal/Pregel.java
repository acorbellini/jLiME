package edu.jlime.graphly.traversal;

import edu.jlime.pregel.graph.VertexFunction;

public class Pregel extends CustomTraversal {

	public Pregel(GraphlyTraversal tr) {
		super(tr);
	}

	public Pregel vertexFunction(VertexFunction func, int steps,
			boolean execOnWholeGraph) {
		tr.customStep(new PregelCustomFunction(func, steps, execOnWholeGraph));
		return this;
	}
}
