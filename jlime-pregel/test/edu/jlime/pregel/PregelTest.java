package edu.jlime.pregel;

import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.worker.WorkerServer;

public class PregelTest {
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
		Vertex v4 = g.vertex();

		g.putLink(v0, v1);
		g.putLink(v0, v2);
		g.putLink(v0, v3);
		g.putLink(v1, v0);
		g.putLink(v1, v2);
		g.putLink(v3, v4);
		g.putLink(v4, v2);

		g.setVal(v0, "count", 0);

		PregelClient cli = new PregelClient(4);
		PregelGraph res = cli.execute(g, new MinTree(), v0);
		System.out.println(res);
		srv.stop();
		w1.stop();
		w2.stop();
		w3.stop();
		w4.stop();
		cli.stop();
	}
}
