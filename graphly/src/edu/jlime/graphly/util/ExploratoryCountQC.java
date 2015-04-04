package edu.jlime.graphly.util;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;

public class ExploratoryCountQC implements QueryContainer {

	@Override
	public void run(GraphlyGraph g, long[] users, Mapper mapper)
			throws Exception {
		int size = 5000;
		if (users.length > 10)
			size = 2500;
		if (users.length > 30)
			size = 1250;

		g.v(users).setPrintSteps(true).set("mapper", mapper)
				.as(Recommendation.class)
				.exploratoryCount(size, 10, Dir.OUT, Dir.IN, Dir.OUT)
				.submit(g.getJobClient().getCluster().getExecutors().get(0));
	}

	@Override
	public String getID() {
		return "exploratory";
	}

}
