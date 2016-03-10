package edu.jlime.graphly.rec;

import edu.jlime.graphly.rec.CustomStep.CustomFunction;
import edu.jlime.graphly.traversal.PregelTraversal;
import edu.jlime.graphly.traversal.Traversal;
import edu.jlime.graphly.traversal.TraversalResult;
import edu.jlime.pregel.client.PregelConfig;
import gnu.trove.set.hash.TLongHashSet;

public class CommonNeighboursPregelStep implements CustomFunction {

	@Override
	public TraversalResult execute(TraversalResult before, Traversal tr)
			throws Exception {
		TLongHashSet vertices = before.vertices();
		int res = vertices.size();

		PregelConfig config = PregelConfig.create().steps(1).aggregator("cm",
				new IntersectAggregator());

		return tr.getGraph().v(vertices).set("mapper", tr.get("mapper"))
				.as(PregelTraversal.class)
				.vertexFunction(new CommonNeighboursPregel(res), config)
				.aggregatorValue("cm").exec();
	}

}
