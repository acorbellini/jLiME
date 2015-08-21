package edu.jlime.graphly.traversal;

import edu.jlime.graphly.rec.CustomStep.CustomFunction;

public class AggregatorStep implements CustomFunction {

	private String agg;

	public AggregatorStep(String string) {
		this.agg = string;
	}

	@Override
	public TraversalResult execute(TraversalResult before, GraphlyTraversal tr)
			throws Exception {
		return new ValueResult(((PregelResult) before).getRes().getAgg(agg)
				.get());
	}

}
