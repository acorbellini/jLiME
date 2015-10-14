package edu.jlime.graphly.traversal;

import edu.jlime.graphly.rec.CustomStep;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.graph.VertexFunction;

public class PregelTraversal extends CustomTraversal {

	public PregelTraversal(Traversal tr) {
		super(tr);
	}

	public PregelTraversal vertexFunction(VertexFunction<?> func, PregelConfig config) {
		tr.customStep(new PregelCustomFunction(func, config));
		return this;
	}

	public PregelConfig getConfig() {
		CustomStep last = (CustomStep) tr.steps.get(tr.steps.size() - 1);
		return ((PregelCustomFunction) last.getFunction()).getConfig();
	}

	public PregelTraversal aggregatorValue(String string) {
		tr.customStep(new AggregatorStep(string));
		return this;
	}

	public PregelTraversal aggregatorSet(String string) {
		tr.customStep(new AggregatorSetStep(string));
		return this;
	}
}
