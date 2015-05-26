package edu.jlime.graphly.traversal;

import edu.jlime.graphly.rec.CustomStep;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public class Pregel extends CustomTraversal {

	public Pregel(GraphlyTraversal tr) {
		super(tr);
	}

	public Pregel vertexFunction(VertexFunction<?> func, PregelConfig config) {
		tr.customStep(new PregelCustomFunction(func, config));
		return this;
	}

	public PregelConfig getConfig() {
		CustomStep last = (CustomStep) tr.steps.get(tr.steps.size() - 1);
		return ((PregelCustomFunction) last.getFunction()).getConfig();
	}
}
