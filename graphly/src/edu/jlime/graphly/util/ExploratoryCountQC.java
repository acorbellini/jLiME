package edu.jlime.graphly.util;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.graphly.rec.Recommendation;
import edu.jlime.graphly.traversal.Dir;

public class ExploratoryCountQC implements QueryContainer {

	@Override
	public void run(Graphly g, long[] users, Mapper mapper) throws Exception {
		int size = 400;
		if (users.length > 10)
			size = 300;
		if (users.length > 30)
			size = 200;

		g.v(users).set("mapper", mapper).as(Recommendation.class)
				.exploratoryCount(size, 10, Dir.OUT, Dir.IN, Dir.OUT)
				.submit(g.getJobClient().getCluster().getExecutors().get(0));
	}

	@Override
	public String getID() {
		return "exploratory";
	}

}
