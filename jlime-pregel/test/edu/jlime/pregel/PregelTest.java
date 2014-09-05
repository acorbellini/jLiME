package edu.jlime.pregel;

import java.util.HashMap;

import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.coordinator.Aggregator;
import edu.jlime.pregel.coordinator.CoordinatorServer;
import edu.jlime.pregel.graph.PregelGraph;
import edu.jlime.pregel.graph.Vertex;
import edu.jlime.pregel.worker.WorkerServer;

public class PregelTest {
	public static void main(String[] args) throws Exception {
		// for (int i = 0; i < 100; i++) {
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
		g.setDefaultValue("pagerank", 1d / g.vertexSize());
		g.setDefaultValue("ranksource", .15d);
		g.putLink(v0, v1);
		g.putLink(v0, v2);
		g.putLink(v0, v3);
		g.putLink(v1, v0);
		g.putLink(v1, v2);
		g.putLink(v2, v4);
		g.putLink(v3, v4);
		g.putLink(v4, v2);
		g.setVal(v0, "count", 0);
		PregelClient cli = new PregelClient(4);
		PregelGraph res = cli.execute(g, new MinTree(), 10, v0);
		System.out.println(res);
		HashMap<String, Aggregator> aggregators = new HashMap<>();
		Aggregator agg = new DifferenceAggregator();
		aggregators.put("diff", agg);
		for (Vertex v : g.vertices()) {
			agg.setVal(v, 1d / g.vertexSize());
		}
		res = cli.execute(g, new PageRank(), 100, aggregators, g.vertices());
		double sum = 0;
		for (Vertex v : res.vertices()) {
			sum += (Double) res.getData(v).getData("pagerank");
		}
		System.out.println(res);
		System.out.println("sum: " + sum + " avg: " + sum / res.vertexSize());
		srv.stop();
		w1.stop();
		w2.stop();
		w3.stop();
		w4.stop();
		cli.stop();
		// }
	}
}
