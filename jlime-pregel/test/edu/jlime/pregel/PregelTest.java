package edu.jlime.pregel;

import edu.jlime.pregel.client.InMemoryGraph;
import edu.jlime.pregel.client.PregelClient;
import edu.jlime.pregel.client.PregelConfig;

public class PregelTest {
	private static final int LIMIT = 50000;

	public static void main(String[] args) throws Exception {
		// for (int i = 0; i < 100; i++) {
		// CoordinatorServer srv = new CoordinatorServer();
		// srv.start();
		// WorkerServer w1 = new WorkerServer();
		// w1.start();
		// WorkerServer w2 = new WorkerServer();
		// w2.start();
		// WorkerServer w3 = new WorkerServer();
		// w3.start();
		// WorkerServer w4 = new WorkerServer();
		// w4.start();

		PregelClient cli = new PregelClient(2);
		InMemoryGraph g = new InMemoryGraph(cli.getRPC(), "graph",
				SplitFunctions.simple(), PregelClient.workerFilter(), 2);

		String pathname = "C:/Users/Ale/Desktop/dataset.csv";
		// g.load(pathname);

		g.putLink(0l, 1l);
		g.putLink(0l, 2l);
		g.putLink(0l, 3l);
		g.putLink(1l, 0l);
		g.putLink(1l, 2l);
		g.putLink(2l, 4l);
		g.putLink(3l, 4l);

		// System.out.println(g.print());

		// g.putLink(4l, 2l);
		// }
		System.out.println("Executing PageRank Test.");

		g.setDefaultValue("pagerank", 1d / g.vertexSize());
		g.setDefaultValue("ranksource", .85d);

		cli.execute(
				new PageRank(g.vertexSize()),
				new PregelConfig().split(SplitFunctions.simple())
						.merger(MessageMergers.sum()).graph(g).steps(30)
						.threads(10).executeOnAll());

		System.out.println("Finished PageRank Test.");

		double sum = 0;

		// If everything's alright, the sum should be 1 (Or near)
		for (Long v : g.vertices()) {
			sum += (Double) g.get(v, "pagerank");
		}
		System.out.println("vertices: " + g.vertexSize() + " sum: " + sum
				+ " avg: " + sum / g.vertexSize());
		System.out.println(g.print());
		// srv.stop();
		// w1.stop();
		// w2.stop();
		// w3.stop();
		// w4.stop();
		cli.stop();
		// }
	}

}
