package edu.jlime.pregel.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.graph.VertexFunction;
import edu.jlime.pregel.worker.WorkerServer;
import edu.jlime.pregel.worker.WorkerTask.StepData;
import edu.jlime.util.DataTypeUtils;

public class Test {
	private static class MinTree implements VertexFunction {

		@Override
		public void execute(Vertex v, StepData value, PregelGraph graph) {
			List<Vertex> sorted = new ArrayList<>(value.adyacents.keySet());
			Collections.sort(sorted, new Comparator<Vertex>() {

				@Override
				public int compare(Vertex o1, Vertex o2) {
					return Integer.compare(DataTypeUtils
							.byteArrayToInt(value.adyacents.get(o1)),
							DataTypeUtils.byteArrayToInt(value.adyacents
									.get(o2)));
				}

			});

		}
	}

	public static void main(String[] args) throws Exception {
		CoordinatorServer srv = new CoordinatorServer();
		srv.start();

		WorkerServer w1 = new WorkerServer();
		w1.start();
		WorkerServer w2 = new WorkerServer();
		w1.start();
		WorkerServer w3 = new WorkerServer();
		w1.start();
		WorkerServer w4 = new WorkerServer();
		w1.start();

		PregelGraph g = new PregelGraph();
		Vertex v1 = g.vertex();
		Vertex v2 = g.vertex();
		Vertex v3 = g.vertex();
		Vertex v4 = g.vertex();
		Vertex v5 = g.vertex();

		g.putLink(v1, v2);
		g.putLink(v1, v3);
		g.putLink(v1, v4);
		g.putLink(v2, v1);
		g.putLink(v2, v3);
		g.putLink(v4, v5);
		g.putLink(v5, v3);

		HashMap<Vertex, byte[]> data = new HashMap<>();
		data.put(v1, DataTypeUtils.intToByteArray(0));

		PregelClient cli = new PregelClient();
		cli.execute(g, data, new MinTree());

	}
}
