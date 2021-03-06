package edu.jlime.graphly.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.text.NumberFormat;
import java.util.Locale;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.server.GraphlyServer;
import edu.jlime.graphly.traversal.PregelTraversal;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.functions.PageRankFloat;
import edu.jlime.pregel.functions.PageRankFloat.PageRankHaltCondition;
import edu.jlime.pregel.mergers.MessageMergers;
import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.TLongFloatMap;

public class LoopbackTest {
	public static void main(String[] args) throws Exception {
		GraphlyServer server = GraphlyServerFactory.loopback(args[0]).build();

		server.start();

		Graphly g = server.getGraphlyClient();

		Graph test = g.getGraph(args[1]);

		// GraphlyLoader loader = new GraphlyLoader(test);
		// loader.load("C:/Users/acorbellini/Desktop/grafo-carlos/in", ",",
		// Dir.IN);
		// loader.load("C:/Users/acorbellini/Desktop/grafo-carlos/out", ",",
		// Dir.OUT);

		int vertexCount = test.getVertexCount();
		System.out.println("Number of vertices: " + vertexCount);
		long init = System.currentTimeMillis();

		// test.setDefaultFloat("pagerank", 1f / vertexCount);
		test.setDefaultFloat("ranksource", .85f);

		test.v().set("mapper", MapperFactory.rr()).as(PregelTraversal.class)
				.vertexFunction(new PageRankFloat("pagerank", vertexCount),
						PregelConfig.create()
								.haltCondition(new PageRankHaltCondition(
										0.000001f, "pr"))
								.steps(50).persistVList(false)
								.executeOnAll(true).cacheSize(100)
								.aggregator("pr", MessageAggregators.floatSum())
								.merger("pr", MessageMergers.floatSum()))
				.exec();
		System.out.println((System.currentTimeMillis() - init) / 1000f);
		NumberFormat numberInstance = NumberFormat.getNumberInstance(Locale.US);
		numberInstance.setMaximumFractionDigits(10);

		BufferedWriter writer = new BufferedWriter(
				new FileWriter(new File(args[2])));

		TLongFloatMap res = test.getFloats("pagerank");
		TLongFloatIterator it = res.iterator();
		while (it.hasNext()) {
			it.advance();
			writer.append(
					it.key() + "," + numberInstance.format(it.value()) + "\n");
		}
		writer.close();
		float sum = test.sumFloat("pagerank");
		System.out.println(sum);

		server.stop();
	}
}
