package edu.jlime.pregel;

import java.util.List;

import org.apache.log4j.Logger;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.PregelMessage;
import edu.jlime.util.PerfMeasure;

public class PageRank implements VertexFunction {

	double error = 0.0001;
	private int vertexSize;

	public PageRank(int vSize) {
		this.vertexSize = vSize;
	}

	// double d = 0.85;

	@Override
	public void execute(long v, List<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		Logger log = Logger.getLogger(PageRank.class);

		Graph graph = ctx.getGraph();

		Double oldval = (Double) graph.get(v, "pagerank");

		// for (PregelMessage incoming : in) {
		// // Cache pagerank in current graph view in case that the incoming
		// // vertex halts.
		// graph.setVal(incoming.getTo(), "edgePageRank",
		// (Double) incoming.getV());
		// }

		// Jacobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		double currentVal = oldval;
		if (ctx.getSuperStep() >= 1) {
			double sum = 0;
			for (PregelMessage pm : in) {
				sum += (Double) pm.getV();
			}

			double d = (Double) graph.get(v, "ranksource");
			currentVal = (1 - d) / vertexSize + d * (sum);
			if (log.isDebugEnabled())
				log.debug("Saving pagerank " + currentVal + " into " + v
						+ " ( 1 - " + d + "/" + graph.vertexSize() + " + " + d
						+ "*" + sum + " )");

			graph.setVal(v, "pagerank", currentVal);

			// If converged, set as halted for the next superstep. The value of
			// the current pagerank was saved in
			// the previous step.
			// if (Math.abs(oldval - currentVal) < error)
			// ctx.setHalted();
		}

		double outgoingSize = (double) graph.getOutgoingSize(v);
		Iterable<Long> outgoing;
		// Dangling nodes distribute pagerank across the whole graph.
		if (outgoingSize == 0) {
			outgoingSize = vertexSize;
			ctx.sendAll(currentVal / outgoingSize);
		} else {
			outgoing = graph.getOutgoing(v);
			for (Long vertex : outgoing) {
				if (log.isDebugEnabled())
					log.debug("Sending message to " + vertex + " from " + v);
				ctx.send(vertex, currentVal / outgoingSize);
			}
		}
	}
}
