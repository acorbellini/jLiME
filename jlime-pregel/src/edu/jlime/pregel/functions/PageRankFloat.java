package edu.jlime.pregel.functions;

import java.util.Iterator;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.messages.FloatPregelMessage;
import edu.jlime.pregel.worker.FloatAggregator;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class PageRankFloat implements VertexFunction<FloatPregelMessage> {
	private static final String PAGERANK_MESSAGE = "pr";

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

		// Jacobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		float currentVal = 1f / vertexSize;
		if (ctx.getSuperStep() >= 1) {
			float oldval = graph.getFloat(v, prop);
			double sum = 0f;
			while (in.hasNext())
				sum += ((FloatPregelMessage) in.next()).getFloat();
			float d = graph.getFloat(v, "ranksource");
			currentVal = (float) ((1 - d) / vertexSize + d * sum);
			float diff = Math.abs(currentVal - oldval);
			FloatAggregator ag = (FloatAggregator) ctx
					.getAggregator(PAGERANK_MESSAGE);
			ag.add(-1, -1, diff);
		}

		graph.setFloat(v, prop, currentVal);

		TLongArrayList outgoing = graph.getOutgoing(v);

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
