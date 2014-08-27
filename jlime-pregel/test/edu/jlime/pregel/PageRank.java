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

	double error = 0.00001;

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

		double currentVal = oldval;
		if (ctx.getSuperStep() >= 1) {
			double sum = 0;
			for (Vertex vertex : graph.getIncoming(v)) {
				sum += (Double) graph.get(vertex, "pagerank");
			}
			// (double) graph.getAdyacencySize(v)
			currentVal = 0.15 + 0.85 * sum;

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
