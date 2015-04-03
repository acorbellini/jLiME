package edu.jlime.graphly.traversal;

import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.MessageMerger;

public class Pregel extends CustomTraversal {

	public Pregel(GraphlyTraversal tr) {
		super(tr);
	}

	public Pregel vertexFunction(VertexFunction func, MessageMerger merger,
			int steps, boolean execOnWholeGraph) {
		tr.customStep(new PregelCustomFunction(func, merger, steps,
				execOnWholeGraph));
		return this;
	}
}
