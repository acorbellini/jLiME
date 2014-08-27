package edu.jlime.pregel;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;
import edu.jlime.pregel.worker.VertexData;

public class PageRank implements VertexFunction {

	double error = 0.001;

	// double d = 0.85;

	@Override
	public void execute(Vertex v, HashSet<PregelMessage> in, WorkerContext ctx)
			throws Exception {
		PregelGraph graph = ctx.getGraph();

		Double oldval = (Double) graph.get(v, "pagerank");

		for (PregelMessage incoming : in) {
			// Cache pagerank in current graph view in case that the incoming
			// vertex halts.
			graph.setVal(incoming.getVertex(), "pagerank",
					(Double) incoming.get("edgePageRank"));
		}

		// Jabobi iterative method: (1-d) + d * function
		// Example :
		// http://mathscinotes.wordpress.com/2012/01/02/worked-pagerank-example/
		double currentVal = oldval;
		if (ctx.getSuperStep() >= 1) {
			double sum = 0;
			for (Vertex vertex : graph.getIncoming(v)) {
				sum += (Double) graph.get(vertex, "pagerank");
			}

			double d = (Double) graph.get(v, "ranksource");

			currentVal = d / graph.vertexSize() + (1 - d) * (sum);
			// + (Double) graph.get(v, "ranksource")
			System.out.println("Saving pagerank " + currentVal + " into " + v);

			graph.setVal(v, "pagerank", currentVal);

			// If converged, set as halted for the next superstep. The value of
			// the current pagerank was saved in
			// the previous step.
			if (Math.abs(oldval - currentVal) < error)
				ctx.setHalted(v);
		}

		Set<Vertex> outgoing = graph.getOutgoing(v);
		for (Vertex vertex : outgoing) {
			ctx.send(
					v,
					vertex,
					VertexData.create("edgePageRank", currentVal
							/ (double) outgoing.size()));
		}

	}
}
