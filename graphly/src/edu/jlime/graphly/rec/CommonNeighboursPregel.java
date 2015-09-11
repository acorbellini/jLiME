package edu.jlime.graphly.rec;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.messages.FloatPregelMessage;
import gnu.trove.set.hash.TLongHashSet;

public class CommonNeighboursPregel implements VertexFunction<FloatPregelMessage> {

	private float targetAmount;

	public CommonNeighboursPregel(float target) {
		this.targetAmount = target;
	}

	@Override
	public void execute(long v, Iterator<FloatPregelMessage> in, WorkerContext ctx) throws Exception {

		TLongHashSet out = ctx.getGraph().getNeighbours(v);
		((SetAggregator) ctx.getAggregator("cm")).add(out);

	}

}
