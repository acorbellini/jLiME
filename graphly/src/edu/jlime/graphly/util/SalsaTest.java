package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.jobs.MapperFactory;

public class SalsaTest {
	public static void main(String[] args) throws Exception {
		Graphly cli = Graphly.build(4);
		Mapper mapper = MapperFactory.location();
		Graph graph = cli.getGraph("konect");
		Algorithms.salsaPregel().run(new long[] { 1 }, graph, mapper)
				.submit(cli.getJobClient().getCluster().getAnyExecutor());
	}
}
