package edu.jlime.pregel;

import java.util.HashSet;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.PregelMessage;
import edu.jlime.pregel.worker.VertexData;

class MinTree implements VertexFunction {

	private static final String VISITED = "visited";
	private static final String NORMAL = "normal";
	private static final String OK = "ok";
	private static final String DELETE = "delete";
	private static final String STATUS = "status";

	@Override
	public void execute(Vertex v, HashSet<PregelMessage> incoming, WorkerContext ctx)
			throws Exception {

		PregelGraph graph = ctx.getGraph();

		if (graph.isTrue(v, VISITED)) {
			System.out.println("Vertex is visited: " + v);
			for (PregelMessage adyacent : incoming) {
				if (adyacent.equals(STATUS, DELETE)) {
					graph.removeLink(v, adyacent.getVertex());
					System.out.println("Deleting link " + v + " -> "
							+ adyacent.getVertex());
				} else if (adyacent.equals(STATUS, OK)) {
					System.out.println("Link " + v + " -> "
							+ adyacent.getVertex() + " is OK");
				} else if (adyacent.equals(STATUS, NORMAL)) {
					System.out.println("Ordering " + adyacent.getVertex()
							+ " to delete this link (if exists): "
							+ adyacent.getVertex() + " -> " + v);
					ctx.send(v, adyacent.getVertex(),
							VertexData.create(STATUS, DELETE));
				}
			}
			return;
		}

		System.out.println("Visiting: " + v);

		graph.setTrue(v, VISITED);

		for (Vertex ady : graph.getOutgoing(v))
			ctx.send(v, ady, VertexData.create(STATUS, NORMAL));

		boolean first = true;
		for (PregelMessage ady : incoming) {
			if (first) {
				ctx.send(v, ady.getVertex(), VertexData.create(STATUS, OK));
				first = false;
			} else {
				ctx.send(v, ady.getVertex(), VertexData.create(STATUS, DELETE));
			}
		}
	}
}