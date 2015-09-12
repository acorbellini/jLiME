package edu.jlime.pregel.functions;

import java.util.Iterator;

import edu.jlime.pregel.client.Context;
import edu.jlime.pregel.coordinator.CoordinatorTask;
import edu.jlime.pregel.coordinator.HaltCondition;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.PregelGraph;
import edu.jlime.pregel.messages.FloatMessage;
import edu.jlime.pregel.worker.FloatAggregator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.set.hash.TLongHashSet;

public class PageRankFloat implements VertexFunction<FloatMessage> {
	private static final String PAGERANK_MESSAGE = "pr";

	private int vertexSize;
	private String prop;

	public static final class PageRankHaltCondition implements HaltCondition {

		private float cut;
		private String msgKey;

		public PageRankHaltCondition(float cut, String msgK) {
			this.cut = cut;
			this.msgKey = msgK;
		}

		@Override
		public boolean eval(CoordinatorTask coordinatorTask, int step) {
			if (step <= 1)
				return false;

			FloatAggregator ag = (FloatAggregator) coordinatorTask.getAggregator(msgKey);
			float f = ag.get();
			return f < cut;
		}
	}

	public PageRankFloat(String pagerankProp, int vSize) {
		this.prop = pagerankProp;
		this.vertexSize = vSize;
	}

	@Override
	public void execute(long v, Iterator<FloatMessage> in, Context ctx) throws Exception {
		PregelGraph graph = ctx.getGraph();

		// Jacobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		float currentVal = 1f / vertexSize;
		if (ctx.getSuperStep() >= 1) {
			float oldval = graph.getFloat(v, prop);
			double sum = 0f;
			while (in.hasNext())
				sum += ((FloatMessage) in.next()).value();
			float d = graph.getFloat(v, "ranksource");
			currentVal = (float) ((1 - d) / vertexSize + d * sum);
			float diff = Math.abs(currentVal - oldval);
			FloatAggregator ag = (FloatAggregator) ctx.getAggregator(PAGERANK_MESSAGE);
			ag.add(-1, -1, diff);
		}

		graph.setFloat(v, prop, currentVal);

		TLongHashSet outgoing = graph.getOutgoing(v);

		// Dangling nodes distribute pagerank across the whole graph.
		if (outgoing.size() == 0) {
			float val = currentVal / vertexSize;
			ctx.sendAllFloat(PAGERANK_MESSAGE, val);
		} else {
			float val = currentVal / outgoing.size();
			TLongIterator oIt = outgoing.iterator();
			while (oIt.hasNext()) {
				long vertex = oIt.next();
				ctx.sendFloat(PAGERANK_MESSAGE, vertex, val);
			}
		}
	}
}
