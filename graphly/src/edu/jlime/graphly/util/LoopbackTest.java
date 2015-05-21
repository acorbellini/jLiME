package edu.jlime.graphly.util;

import java.util.List;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.MapperFactory;
import edu.jlime.graphly.traversal.Pregel;
import edu.jlime.pregel.client.PregelConfig;
import edu.jlime.pregel.functions.PageRankFloat;
import edu.jlime.pregel.mergers.MessageMergers;

public class LoopbackTest {
	public static void main(String[] args) throws Exception {
		Graphly g = Graphly.buildLocal("D:/Graphly/konect");

		GraphlyGraph test = g.getGraph("konect");

		// GraphlyLoader loader = new GraphlyLoader(test);
		// loader.load("D:/Graphly/konect.in", ",", Dir.IN);
		// loader.load("D:/Graphly/konect.out", ",", Dir.OUT);

		int vertexCount = test.getVertexCount();
		System.out.println("Number of vertices: " + vertexCount);
		long init = System.currentTimeMillis();

		test.setDefaultFloat("pagerank", 1f / vertexCount);
		test.setDefaultFloat("ranksource", .85f);

		test.v()
				.set("mapper", MapperFactory.rr())
				.as(Pregel.class)
				.vertexFunction(
						new PageRankFloat("pagerank", vertexCount),
						PregelConfig.create().steps(10).executeOnAll(true)
								.merger("pr", MessageMergers.FLOAT_SUM)).exec();
		System.out.println(System.currentTimeMillis() - init);
		float sum = 0;
		List<Float> vals = test
				.gather(new SumFloatPropertiesGather("pagerank"));
		for (Float float1 : vals) {
			sum += float1;
		}

		System.out.println(sum);

		g.close();
	}
}
