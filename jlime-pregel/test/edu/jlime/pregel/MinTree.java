package edu.jlime.pregel;

import java.util.List;

import edu.jlime.pregel.client.WorkerContext;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.graph.rpc.Graph;
import edu.jlime.pregel.worker.PregelMessage;

class MinTree implements VertexFunction {

	private static final String VISITED = "visited";
	private static final String NORMAL = "normal";
	private static final String OK = "ok";
	private static final String DELETE = "delete";

	@Override
	public void execute(long v, List<PregelMessage> incoming, WorkerContext ctx)
			throws Exception {

		Graph graph = ctx.getGraph();

		if (graph.get(v, VISITED).equals(Boolean.TRUE)) {
			System.out.println("Vertex is visited: " + v);
			for (PregelMessage adyacent : incoming) {
				if (adyacent.getV().equals(DELETE)) {
					graph.removeOutgoing(v, adyacent.getTo());
					System.out.println("Deleting link " + v + " -> "
							+ adyacent.getTo());
				} else if (adyacent.getV().equals(OK)) {
					System.out.println("Link " + v + " -> " + adyacent.getTo()
							+ " is OK");
				} else if (adyacent.getV().equals(NORMAL)) {
					System.out.println("Ordering " + adyacent.getTo()
							+ " to delete this link (if exists): "
							+ adyacent.getTo() + " -> " + v);
					ctx.send(adyacent.getTo(), DELETE);
				}
			}
			return;
		}

		System.out.println("Visiting: " + v);

		graph.setVal(v, VISITED, Boolean.TRUE);
		for (Long dest : graph.getOutgoing(v)) {
			ctx.send(dest, NORMAL);
		}

		boolean first = true;
		for (PregelMessage ady : incoming) {
			if (first) {
				ctx.send(ady.getTo(), OK);
				first = false;
			} else {
				ctx.send(ady.getTo(), DELETE);
			}
		}
	}
}