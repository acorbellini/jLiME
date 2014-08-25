package edu.jlime.pregel;

import java.util.HashMap;
import java.util.Map.Entry;

import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.client.TaskContext;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.VertexData;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.WorkerTask.StepData;

public class PregelTest {
	private static class MinTree implements VertexFunction {

		@Override
		public void execute(Vertex v, StepData value, TaskContext ctx)
				throws Exception {

			if (ctx.getGraph().isTrue(v, "visited")) {
				System.out.println("Vertex is visited: " + v);
				for (Entry<Vertex, VertexData> e : value.adyacents.entrySet()) {
					Object data = e.getValue().getData("status");
					if (data != null) {
						if (data.equals("delete")) {
							ctx.getGraph().removeLink(v, e.getKey());
							System.out.println("Deleting link " + v + " -> "
									+ e.getKey());
						} else if (data.equals("ok")) {
							System.out.println("Link " + v + " -> "
									+ e.getKey() + " is OK");
						}

					} else {
						System.out.println("Ordering " + e.getKey()
								+ " to delete this link (if exists): "
								+ e.getKey() + " -> " + v);
						ctx.send(v, e.getKey(),
								VertexData.create("status", "delete"));
					}
				}
				return;
			}
			System.out.println("Visiting: " + v);
			ctx.getGraph().setTrue(v, "visited");

			for (Vertex ady : ctx.getGraph().getAdyacency(v))
				ctx.send(v, ady, new VertexData());
			boolean first = true;
			for (Entry<Vertex, VertexData> e : value.adyacents.entrySet()) {
				if (first) {
					ctx.send(v, e.getKey(), VertexData.create("status", "ok"));
					first = false;
				} else {
					ctx.send(v, e.getKey(),
							VertexData.create("status", "delete"));
				}
			}

		}
	}

	public static void main(String[] args) throws Exception {
		CoordinatorServer srv = new CoordinatorServer();
		srv.start();

		WorkerServer w1 = new WorkerServer();
		w1.start();
		WorkerServer w2 = new WorkerServer();
		w2.start();
		WorkerServer w3 = new WorkerServer();
		w3.start();
		WorkerServer w4 = new WorkerServer();
		w4.start();

		PregelGraph g = new PregelGraph();
		Vertex v0 = g.vertex();
		Vertex v1 = g.vertex();
		Vertex v2 = g.vertex();
		Vertex v3 = g.vertex();
		Vertex v5 = g.vertex();

		g.putLink(v0, v1);
		g.putLink(v0, v2);
		g.putLink(v0, v3);
		g.putLink(v1, v0);
		g.putLink(v1, v2);
		g.putLink(v3, v5);
		g.putLink(v5, v2);

		HashMap<Vertex, VertexData> data = new HashMap<>();
		VertexData value = new VertexData();
		value.put("count", 0);
		data.put(v0, value);

		PregelClient cli = new PregelClient(4);
		PregelGraph res = cli.execute(g, data, new MinTree());
		System.out.println(res);
		srv.stop();
		w1.stop();
		w2.stop();
		w3.stop();
		w4.stop();
		cli.stop();
	}
}
