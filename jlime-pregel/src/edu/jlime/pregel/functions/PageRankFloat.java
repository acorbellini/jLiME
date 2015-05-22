package edu.jlime.pregel.functions;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.worker.FloatAggregator;
import gnu.trove.iterator.TLongIterator;

public class PageRankFloat implements VertexFunction<FloatPregelMessage> {
	private int vertexSize;
	private String prop;

	public PageRankFloat(String pagerankProp, int vSize) {
		this.prop = pagerankProp;
		this.vertexSize = vSize;
	}

	@Override
	public void execute(long v, Iterator<FloatPregelMessage> in,
			WorkerContext ctx) throws Exception {
		Graph graph = ctx.getGraph();

		float oldval = graph.getFloat(v, prop);

		// Jacobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		float currentVal = oldval;
		if (ctx.getSuperStep() >= 1) {
			float sum = 0f;
			while (in.hasNext())
				sum += ((FloatPregelMessage) in.next()).getFloat();

			float d = graph.getFloat(v, "ranksource");

			currentVal = (1 - d) / vertexSize + d * sum;

			float diff = currentVal - oldval;

			FloatAggregator ag = (FloatAggregator) ctx.getAggregator("pr");

			ag.add(-1, -1, diff);

			graph.setFloat(v, prop, currentVal);
		}

		int outgoingSize = graph.getOutgoingSize(v);

		// Dangling nodes distribute pagerank across the whole graph.
		if (outgoingSize == 0) {
			float val = currentVal / vertexSize;
			ctx.sendAllFloat("pr", val);
		} else {
			float val = currentVal / outgoingSize;
			TLongIterator outgoing = graph.getOutgoing(v).iterator();
			while (outgoing.hasNext()) {
				long vertex = outgoing.next();
				ctx.sendFloat("pr", vertex, val);
			}
		}
	}
}
