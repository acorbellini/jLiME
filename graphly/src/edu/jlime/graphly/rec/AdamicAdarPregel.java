package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.graphly.util.FloatSumAggregator;
import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.messages.FloatMessage;

public class AdamicAdarPregel implements VertexFunction<FloatMessage> {

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx) throws Exception {
		float count = (float) (1 / Math.log(ctx.getGraph().getNeighbourhoodSize(v)));
		((FloatSumAggregator) ctx.getAggregator("adamic")).add(-1, -1, count);
	}

}
