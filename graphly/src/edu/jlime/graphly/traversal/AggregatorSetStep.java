package edu.jlime.graphly.traversal;

import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.rec.SetAggregator;

public class AggregatorSetStep implements CustomFunction {

	private String key;

	public AggregatorSetStep(String string) {
		this.key = string;
	}

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr) throws Exception {
		return new VertexResult(((SetAggregator) ((PregelResult) before).getRes().getAgg(key)).getSet());
	}

}
